/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.coordinator.group.assignor;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.common.TopicIdPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.IntStream;

import static java.lang.Math.min;

/**
 * Assigns partitions to members of a consumer group ensuring a balanced distribution with
 * considerations for sticky assignments and rack-awareness.
 * The order of priority of properties during the assignment will be:
 *      balance > rack matching (when applicable) > stickiness.
 */
public class OptimizedUniformAssignmentBuilder extends UniformAssignor.AbstractAssignmentBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(OptimizedUniformAssignmentBuilder.class);
    /**
     * The assignment specification which includes member metadata.
     */
    private final AssignmentSpec assignmentSpec;
    /**
     * The topic and partition metadata describer.
     */
    private final SubscribedTopicDescriber subscribedTopicDescriber;
    /**
     * The set of topic Ids that the consumer group is subscribed to.
     */
    private final Set<Uuid> subscriptionIds;
    /**
     * Rack information.
     */
    private final RackInfo rackInfo;
    /**
     * The number of members to receive an extra partition beyond the minimum quota,
     * to account for the distribution of the remaining partitions.
     */
    private int remainingMembersToGetAnExtraPartition;
    /**
     * Members mapped to the remaining number of partitions needed to meet the minimum quota,
     * including members eligible for an extra partition.
     */
    private final Map<String, Integer> potentiallyUnfilledMembers;
    /**
     * Members mapped to the remaining number of partitions needed to meet the full quota.
     * Full quota = minQuota + one extra partition (if applicable).
     */
    private Map<String, Integer> unfilledMembers;
    /**
     * The partitions that still need to be assigned.
     */
    private List<TopicIdPartition> unassignedPartitions;
    /**
     * The new assignment that will be returned.
     */
    private final Map<String, MemberAssignment> newAssignment;
    /**
     * Tracks the current owner of each partition.
     * Current refers to the existing assignment.
     * Only used when the rack awareness strategy is used.
     */
    private final Map<TopicIdPartition, String> currentPartitionOwners;

    OptimizedUniformAssignmentBuilder(AssignmentSpec assignmentSpec, SubscribedTopicDescriber subscribedTopicDescriber) {
        this.assignmentSpec = assignmentSpec;
        this.subscribedTopicDescriber = subscribedTopicDescriber;
        this.subscriptionIds = new HashSet<>(assignmentSpec.members().values().iterator().next().subscribedTopicIds());
        this.rackInfo = new RackInfo(assignmentSpec, subscribedTopicDescriber, subscriptionIds);
        this.potentiallyUnfilledMembers = new HashMap<>();
        this.unfilledMembers = new HashMap<>();
        this.newAssignment = new HashMap<>();
        // Without rack-aware strategy, tracking current owners of unassigned partitions is unnecessary
        // as all sticky partitions are retained until a member meets its quota.
        this.currentPartitionOwners = rackInfo.useRackStrategy ? new HashMap<>() : Collections.emptyMap();
    }

    /**
     * Here's the step-by-step breakdown of the assignment process:
     *
     * <li> Compute the quotas of partitions for each member based on the total partitions and member count.</li>
     * <li> For existing assignments, retain partitions based on the determined quota and member's rack compatibility.</li>
     * <li> If a partition's rack mismatches with its owner, track it for future use.</li>
     * <li> Identify members that haven't fulfilled their partition quota or are eligible to receive extra partitions.</li>
     * <li> Derive the unassigned partitions by taking the difference between total partitions and the sticky assignments.</li>
     * <li> Depending on members needing extra partitions, select members from the potentially unfilled list
     *      and add them to the unfilled list.</li>
     * <li> Proceed with a round-robin assignment adhering to rack awareness.
     *      For each unassigned partition, locate the first compatible member from the unfilled list.</li>
     * <li> If no rack-compatible member is found, revert to the tracked current owner.
     *      If that member can't accommodate the partition due to quota limits, resort to a generic round-robin assignment.</li>
     */
    @Override
    protected GroupAssignment buildAssignment() throws PartitionAssignorException {
        int totalPartitionsCount = 0;

        for (Uuid topicId : subscriptionIds) {
            int partitionCount = subscribedTopicDescriber.numPartitions(topicId);
            if (partitionCount == -1) {
                throw new PartitionAssignorException(
                    "Members are subscribed to topic " + topicId + " which doesn't exist in the topic metadata."
                );
            } else {
                totalPartitionsCount += partitionCount;
            }
        }

        if (subscriptionIds.isEmpty()) {
            LOG.info("The subscription list is empty, returning an empty assignment");
            return new GroupAssignment(Collections.emptyMap());
        }

        // The minimum required quota that each member needs to meet for a balanced assignment.
        // This is the same for all members.
        final int numberOfMembers = assignmentSpec.members().size();
        final int minQuota = totalPartitionsCount / numberOfMembers;
        remainingMembersToGetAnExtraPartition = totalPartitionsCount % numberOfMembers;

        assignmentSpec.members().keySet().forEach(memberId ->
            newAssignment.put(memberId, new MemberAssignment(new HashMap<>())
        ));

        Set<TopicIdPartition> allAssignedStickyPartitions = computeAssignedStickyPartitions(minQuota);
        unassignedPartitions = computeUnassignedPartitions(allAssignedStickyPartitions);
        unfilledMembers = computeUnfilledMembers();

        if (!unassignedPartitionsCountEqualsRemainingAssignmentsCount()) {
            throw new PartitionAssignorException("Number of available partitions is not equal to the total requirement");
        }

        if (rackInfo.useRackStrategy) rackAwareRoundRobinAssignment();
        unassignedPartitionsRoundRobinAssignment();

        return new GroupAssignment(newAssignment);
    }

    /**
     * Retrieves a set of partitions that were currently assigned to members and will be retained in the new assignment,
     * by ensuring that the partitions are still relevant based on current topic metadata and subscriptions.
     * If rack awareness is enabled, it ensures that a partition's rack matches the member's rack.
     *
     * <p> For each member, it:
     * <ul>
     *     <li> Finds the valid current assignment considering topic subscriptions and metadata.</li>
     *     <li> If current assignments exist, retains up to the minimum quota of assignments.</li>
     *     <li> If there are members that should get an extra partition,
     *          assigns the next partition after the retained ones.</li>
     *     <li> For members with assignments not exceeding the minimum quota,
     *          it identifies them as potentially unfilled members and tracks the remaining quota.</li>
     * </ul>
     *
     * @return A set containing all the sticky partitions that have been retained in the new assignment.
     */
    private Set<TopicIdPartition> computeAssignedStickyPartitions(int minQuota) {
        Set<TopicIdPartition> allAssignedStickyPartitions = new HashSet<>();

        assignmentSpec.members().forEach((memberId, assignmentMemberSpec) -> {
            // Remove all the topics that aren't in the subscriptions or the topic metadata anymore.
            // If rack awareness is enabled, only add partitions if the members rack matches the partitions rack.
            List<TopicIdPartition> validCurrentAssignment = validCurrentAssignment(
                memberId,
                assignmentMemberSpec.assignedPartitions()
            );

            int currentAssignmentSize = validCurrentAssignment.size();
            int remaining = minQuota - currentAssignmentSize;

            if (currentAssignmentSize > 0) {
                int retainedPartitionsCount = min(currentAssignmentSize, minQuota);
                IntStream.range(0, retainedPartitionsCount).forEach(i -> {
                    newAssignment.get(memberId)
                        .targetPartitions()
                        .computeIfAbsent(validCurrentAssignment.get(i).topicId(), __ -> new HashSet<>())
                        .add(validCurrentAssignment.get(i).partition());
                    allAssignedStickyPartitions.add(validCurrentAssignment.get(i));
                });

                // The extra partition is located at the last index from the previous step.
                if (remaining < 0 && remainingMembersToGetAnExtraPartition > 0) {
                    newAssignment.get(memberId)
                        .targetPartitions()
                        .computeIfAbsent(validCurrentAssignment.get(retainedPartitionsCount).topicId(), __ -> new HashSet<>())
                        .add(validCurrentAssignment.get(retainedPartitionsCount).partition());
                    allAssignedStickyPartitions.add(validCurrentAssignment.get(retainedPartitionsCount));
                    remainingMembersToGetAnExtraPartition--;
                }
            }

            if (remaining >= 0) {
                potentiallyUnfilledMembers.put(memberId, remaining);
            }

        });

        return allAssignedStickyPartitions;
    }

    /**
     * Filters the current assignment of partitions for a given member.
     *
     * If a partition is assigned to a member not subscribed to its topic or
     * if the rack-aware strategy is to be used but there is a mismatch,
     * the partition is excluded from the valid assignment and stored for future consideration.
     *
     * @param memberId              The Id of the member whose assignment is being validated.
     * @param assignedPartitions    The partitions currently assigned to the member.
     *
     * @return List of valid partitions after applying the filters.
     */
    private List<TopicIdPartition> validCurrentAssignment(
        String memberId,
        Map<Uuid, Set<Integer>> assignedPartitions
    ) {
        List<TopicIdPartition> validCurrentAssignmentList = new ArrayList<>();
        assignedPartitions.forEach((topicId, currentAssignment) -> {
            if (subscriptionIds.contains(topicId)) {
                currentAssignment.forEach(partition -> {
                    TopicIdPartition topicIdPartition = new TopicIdPartition(topicId, partition);
                    if (rackInfo.useRackStrategy && rackInfo.racksMismatch(memberId, topicIdPartition)) {
                        currentPartitionOwners.put(topicIdPartition, memberId);
                    } else {
                        validCurrentAssignmentList.add(topicIdPartition);
                    }
                });
            } else {
                LOG.debug("The topic " + topicId + " is no longer present in the subscribed topics list");
            }
        });

        return validCurrentAssignmentList;
    }

    /**
     * This method iterates over the unassigned partitions and attempts to allocate them
     * to members while considering their rack affiliations.
     */
    private void rackAwareRoundRobinAssignment() {
        Queue<String> roundRobinMembers = new LinkedList<>(unfilledMembers.keySet());

        // Sort partitions in ascending order by number of potential members with matching racks.
        // Partitions with no potential members aren't included in this list.
        List<TopicIdPartition> sortedPartitions = rackInfo.sortPartitionsByRackMembers(unassignedPartitions);

        sortedPartitions.forEach(partition -> {
            boolean assigned = false;
            for (int i = 0; i < roundRobinMembers.size() && !assigned; i++) {
                String memberId = roundRobinMembers.poll();
                Integer remainingPartitionCount = unfilledMembers.getOrDefault(memberId, 0);

                if (remainingPartitionCount > 0 && !rackInfo.racksMismatch(memberId, partition)) {
                    assignPartitionToMember(memberId, partition);
                    assigned = true;
                    unassignedPartitions.remove(partition);
                }

                // Only re-add to the end of the queue if it's still in the unfilledMembers map
                if (unfilledMembers.containsKey(memberId)) {
                    roundRobinMembers.add(memberId);
                }
            }
        });
    }

    /**
     * Allocates the unassigned partitions to available members.
     *
     * If the rack-aware strategy is enabled, partitions are attempted to be assigned back to their current owners first.
     *
     * If a partition couldn't be assigned to its current owner due to quotas or
     * if the rack-aware strategy is not enabled, the partitions are allocated to members in a round-robin fashion.
     */
    private void unassignedPartitionsRoundRobinAssignment() {
        Queue<String> roundRobinMembers = new LinkedList<>(unfilledMembers.keySet());

        unassignedPartitions.forEach(partition -> {
            boolean assigned = false;

            if (rackInfo.useRackStrategy && currentPartitionOwners.containsKey(partition)) {
                String prevOwner = currentPartitionOwners.get(partition);
                if (unfilledMembers.containsKey(prevOwner)) {
                    assignPartitionToMember(prevOwner, partition);
                    assigned = true;
                    if (!unfilledMembers.containsKey(prevOwner)) {
                        roundRobinMembers.remove(prevOwner);
                    }
                }
            }

            // Only re-add the member to the end of the queue if it's still available for assignment.
            for (int i = 0; i < roundRobinMembers.size() && !assigned; i++) {
                String memberId = roundRobinMembers.poll();
                if (unfilledMembers.get(memberId) > 0) {
                    assignPartitionToMember(memberId, partition);
                    assigned = true;
                }
                if (unfilledMembers.containsKey(memberId)) {
                    roundRobinMembers.add(memberId);
                }
            }
        });
    }

    /**
     * Assigns the specified partition to the given member.
     *
     * <p>
     * If the member has met their allocation quota, the member is removed from the
     * tracking map of members with their remaining allocations.
     * Otherwise, the count of remaining partitions that can be assigned to the member is updated.
     * </p>
     *
     * @param memberId      The Id of the member to which the partition will be assigned.
     * @param partition     The partition to be assigned.
     */
    private void assignPartitionToMember(String memberId, TopicIdPartition partition) {
        newAssignment.get(memberId)
            .targetPartitions()
            .computeIfAbsent(partition.topicId(), __ -> new HashSet<>())
            .add(partition.partition());

        int remainingPartitionCount = unfilledMembers.get(memberId) - 1;
        if (remainingPartitionCount == 0) {
            unfilledMembers.remove(memberId);
        } else {
            unfilledMembers.put(memberId, remainingPartitionCount);
        }
    }

    /**
     * Determines which members can still be assigned partitions to meet the full quota.
     *
     * @return A map of member IDs and their capacity for additional partitions.
     */
    private Map<String, Integer> computeUnfilledMembers() {
        Map<String, Integer> unfilledMembers = new HashMap<>();

        potentiallyUnfilledMembers.forEach((memberId, remaining) -> {
            if (remainingMembersToGetAnExtraPartition > 0) {
                remaining++;
                remainingMembersToGetAnExtraPartition--;
            }
            if (remaining > 0) {
                unfilledMembers.put(memberId, remaining);
            }
        });

        return unfilledMembers;
    }

    /**
     * This method compares the full list of partitions against the set of already
     * assigned sticky partitions to identify those that still need to be allocated.
     *
     * @param allAssignedStickyPartitions   Set of partitions that have already been assigned.
     * @return List of unassigned partitions.
     */
    private List<TopicIdPartition> computeUnassignedPartitions(Set<TopicIdPartition> allAssignedStickyPartitions) {
        List<TopicIdPartition> unassignedPartitions = new ArrayList<>();
        List<Uuid> sortedAllTopics = new ArrayList<>(subscriptionIds);
        Collections.sort(sortedAllTopics);

        if (allAssignedStickyPartitions.isEmpty()) {
            return allTopicIdPartitions(sortedAllTopics, subscribedTopicDescriber);
        }

        sortedAllTopics.forEach(topic ->
            IntStream.range(0, subscribedTopicDescriber.numPartitions(topic))
                .mapToObj(i -> new TopicIdPartition(topic, i))
                .filter(partition -> !allAssignedStickyPartitions.contains(partition))
                .forEach(unassignedPartitions::add)
        );

        return unassignedPartitions;
    }

    /**
     * This method is a correctness check to validate that the number of unassigned partitions
     * is equal to the sum of all the remaining partitions count
     */
    private boolean unassignedPartitionsCountEqualsRemainingAssignmentsCount() {
        int totalRemaining = unfilledMembers.values().stream().reduce(0, Integer::sum);
        return totalRemaining == unassignedPartitions.size();
    }
}
