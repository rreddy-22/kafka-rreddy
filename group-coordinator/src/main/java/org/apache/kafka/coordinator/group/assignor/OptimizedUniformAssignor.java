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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static java.lang.Math.min;

/**
 * <p>
 * Only used when all members have identical subscriptions.
 * Steps followed to get the most sticky and balanced assignment possible :-
 * <ol>
 *      <li> In case of a reassignment i.e. when a previous assignment exists: </li>
 *      <ul>
 *          <li> Obtain a valid prev assignment by selecting the assignments that have topics present in both the topic metadata and the members subscriptions.</li>
 *          <li> Get sticky partitions from the prev valid assignment using the newly decided quotas.</li>
 *      </ul>
 *      <li> Obtain the unassigned partitions from the difference between total partitions and assigned sticky partitions.</li>
 *      <li> Obtain a list of potentially unfilled members based on the minimum quotas.</li>
 *      <li> Populate the unfilled members map (member, remaining) after accounting for the additional partitions that might have to be assigned. </li>
 *      <li> Allocate all unassigned partitions to the unfilled members. </li>
 * </ol>
 * </p>
 */
public class OptimizedUniformAssignor extends UniformAssignor {
    private static final Logger log = LoggerFactory.getLogger(OptimizedUniformAssignor.class);
    // Subscription list is the same for all members.
    private final List<Uuid> subscriptionList;
    private final AssignmentSpec assignmentSpec;
    private final SubscribedTopicDescriber subscribedTopicDescriber;
    private final RackInfo rackInfo;
    private int totalValidPartitionsCount;
    // The minimum required quota that each member needs to meet.
    private final int minQuota;
    // The expected number of members receiving one more than the minQuota partitions.
    private int expectedNumMembersWithExtraPartition;
    // Members that haven't met the min quota OR that have met the min quota but could potentially get an extra partition.
    // Map<memberId, remaining> where remaining = number of partitions remaining to meet the min Quota.
    private final Map<String, Integer> potentiallyUnfilledMembers;
    // Members that need to be assigned the remaining number of partitions including extra partitions.
    private Map<String, Integer> unfilledMembers;
    // Partitions that are available to be assigned, computed by taking the difference between total partitions and assigned sticky partitions.
    private List<TopicIdPartition> unassignedPartitions;

    private final Map<String, MemberAssignment> newAssignment;
    // Partition to previous owner, used only with rack aware strategy.
    private final Map<TopicIdPartition, String> partitionToPrevOwner;

    // If consumer and partition rack information is available this is true.
    boolean useRackAwareStrategy;

    OptimizedUniformAssignor(AssignmentSpec assignmentSpec, SubscribedTopicDescriber subscribedTopicDescriber) {
        this.subscribedTopicDescriber = subscribedTopicDescriber;
        this.assignmentSpec = assignmentSpec;

        subscriptionList = new ArrayList<>(assignmentSpec.members().values().iterator().next().subscribedTopicIds());
        if (subscriptionList.isEmpty()) {
            log.info("Subscriptions list is empty, returning empty assignment");
            throw new PartitionAssignorException("There are no valid topic subscriptions");
        }

        totalValidPartitionsCount = subscriptionList.stream()
            .mapToInt(subscribedTopicDescriber::numPartitions)
            .sum();

        RackInfo rackInfo = new RackInfo(assignmentSpec, subscribedTopicDescriber, subscriptionList);
        this.rackInfo = rackInfo;

        if (rackInfo.consumerRacks.isEmpty() || rackInfo.partitionRacks.isEmpty()) {
            this.useRackAwareStrategy = false;
            partitionToPrevOwner = Collections.emptyMap();
        } else {
            this.useRackAwareStrategy = true;
            partitionToPrevOwner = new HashMap<>();
        }

        potentiallyUnfilledMembers = new HashMap<>();
        unfilledMembers = new HashMap<>();
        newAssignment = new HashMap<>();

        int numberOfMembers = assignmentSpec.members().size();
        minQuota = (int) Math.floor(((double) totalValidPartitionsCount) / numberOfMembers);
        expectedNumMembersWithExtraPartition = totalValidPartitionsCount % numberOfMembers;
    }

     protected GroupAssignment build() {

        assignmentSpec.members().forEach((memberId, assignmentMemberSpec) ->
            newAssignment.put(memberId, new MemberAssignment(new HashMap<>()))
        );

        Set<TopicIdPartition> allAssignedStickyPartitions = getAssignedStickyPartitions();

        unassignedPartitions = getUnassignedPartitions(allAssignedStickyPartitions);

        unfilledMembers = getUnfilledMembers();

        if (!ensureTotalUnassignedPartitionsEqualsTotalRemainingAssignments()) {
            log.warn("Number of available partitions is not equal to the total requirement");
        }
        if (useRackAwareStrategy) rackAwareRoundRobinAssignment();
        unassignedPartitionsRoundRobinAssignment();

        return new GroupAssignment(newAssignment);
    }

    /**
     * Retains valid existing partition assignments and identifies members that may require more partitions.
     *
     * <p>The method filters and retains partitions by:
     * <ul>
     *   <li> Validating each member's current assignments against available subscriptions.</li>
     *   <li> If rack awareness is enabled, ensuring partitions' racks match the consumer member's rack.</li>
     * </ul>
     * Members potentially requiring more partitions to meet the minimum quota are also identified.
     *
     * @return A set of validly retained partitions across all members.
     */
    private Set<TopicIdPartition> getAssignedStickyPartitions() {
        Set<TopicIdPartition> allAssignedStickyPartitions = new HashSet<>();

        assignmentSpec.members().forEach((memberId, assignmentMemberSpec) -> {
            // Remove all the topics that aren't in the subscriptions or the topic metadata anymore.
            // If rack awareness is enabled, only add partitions if the consumers rack matches the partitions rack.
            List<TopicIdPartition> validCurrentAssignment = getValidCurrentAssignment(memberId, assignmentMemberSpec.assignedPartitions());

            int currentAssignmentSize = validCurrentAssignment.size();
            int remaining = minQuota - currentAssignmentSize;

            if (currentAssignmentSize > 0) {

                int retainedPartitionsCount = min(currentAssignmentSize, minQuota);
                for (int i = 0; i < retainedPartitionsCount; i++) {
                    newAssignment.get(memberId)
                        .targetPartitions()
                        .computeIfAbsent(validCurrentAssignment.get(i).topicId(), k -> new HashSet<>())
                        .add(validCurrentAssignment.get(i).partition());
                    allAssignedStickyPartitions.add(validCurrentAssignment.get(i));
                }

                // The last index from the previous step is at int retainedPartitionsCount which is where the extra partition resides.
                if (remaining < 0 && expectedNumMembersWithExtraPartition > 0) {
                    newAssignment.get(memberId)
                        .targetPartitions()
                        .computeIfAbsent(validCurrentAssignment.get(retainedPartitionsCount).topicId(), k -> new HashSet<>())
                        .add(validCurrentAssignment.get(retainedPartitionsCount).partition());
                    allAssignedStickyPartitions.add(validCurrentAssignment.get(retainedPartitionsCount));
                    expectedNumMembersWithExtraPartition--;
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
     *
     * @throws PartitionAssignorException if the member is subscribed to a topic not present in the topic metadata.
     */
    private List<TopicIdPartition> getValidCurrentAssignment(String memberId, Map<Uuid, Set<Integer>> assignedPartitions) {
        List<TopicIdPartition> validCurrentAssignmentList = new ArrayList<>();

        assignedPartitions.forEach((topicId, currentAssignment) -> {
            List<Integer> currentAssignmentList = new ArrayList<>(currentAssignment);

            if (subscriptionList.contains(topicId)) {
                for (Integer partition : currentAssignmentList) {
                    TopicIdPartition topicIdPartition = new TopicIdPartition(topicId, partition);
                    if (useRackAwareStrategy && rackInfo.racksMismatch(memberId, topicIdPartition)) {
                        partitionToPrevOwner.put(topicIdPartition, memberId);
                    } else {
                        validCurrentAssignmentList.add(topicIdPartition);
                    }
                }
            }
            if (subscribedTopicDescriber.numPartitions(topicId) == -1) {
                throw new PartitionAssignorException("Members are subscribed to topic " +
                        topicId + " which doesn't exist in the topic metadata");
            }
        });

        return validCurrentAssignmentList;
    }

    /**
     * This method iterates over unassigned partitions and attempts to allocate them
     * to members while considering their rack affiliations.
     */
    private void rackAwareRoundRobinAssignment() {
        Queue<String> roundRobinMembers = new LinkedList<>(unfilledMembers.keySet());

        // Sorts partitions in ascending order by number of potential consumers in with matching racks.
        rackInfo.sortPartitionsByRackConsumers(unassignedPartitions);

        Iterator<TopicIdPartition> partitionIterator = unassignedPartitions.iterator();
        while (partitionIterator.hasNext()) {
            TopicIdPartition partition = partitionIterator.next();
            boolean assigned = false;

            for (int i = 0; i < unfilledMembers.size() && !assigned; i++) {
                String memberId = roundRobinMembers.poll();

                if (unfilledMembers.get(memberId) > 0 && !rackInfo.racksMismatch(memberId, partition)) {
                    assignPartitionToMember(memberId, partition);
                    assigned = true;
                    partitionIterator.remove();
                }
                roundRobinMembers.add(memberId);
            }
        }
    }

    /**
     * Allocates the unassigned partitions to available members.
     *
     * <p>If the rack-aware strategy is enabled, partitions are first tried to be assigned back to their previous owners.
     * If a partition couldn't be assigned to its previous owner or if the rack-aware strategy is not enabled,
     * the partitions are allocated to members in a round-robin fashion.</p>
     */
    private void unassignedPartitionsRoundRobinAssignment() {
        Queue<String> roundRobinMembers = new LinkedList<>(unfilledMembers.keySet());
        Iterator<TopicIdPartition> partitionIterator = unassignedPartitions.iterator();

        while (partitionIterator.hasNext()) {
            TopicIdPartition partition = partitionIterator.next();
            boolean assigned = false;

            if (useRackAwareStrategy && partitionToPrevOwner.containsKey(partition)) {
                String prevOwner = partitionToPrevOwner.get(partition);
                if (unfilledMembers.containsKey(prevOwner)) {
                    assignPartitionToMember(prevOwner, partition);
                    assigned = true;
                    partitionIterator.remove();
                }
            }

            for (int i = 0; i < unfilledMembers.size() && !assigned; i++) {
                String memberId = roundRobinMembers.poll();

                if (unfilledMembers.get(memberId) > 0) {
                    assignPartitionToMember(memberId, partition);
                    assigned = true;
                    partitionIterator.remove();
                }

                roundRobinMembers.add(memberId);
            }
        }
    }

    /**
     * Assigns the specified partition to the given member.
     *
     * <p>
     * If the member has met their allocation quota, the member is removed from the tracking map of members
     * with their remaining allocations.
     * Otherwise, the count of remaining partitions that can be assigned to the member is updated.
     * </p>
     *
     * @param memberId  The Id of the member to which the partition will be assigned.
     * @param partition The partition to be assigned.
     */
    private void assignPartitionToMember(String memberId, TopicIdPartition partition) {
        newAssignment.get(memberId)
            .targetPartitions()
            .computeIfAbsent(partition.topicId(), k -> new HashSet<>())
            .add(partition.partition());

        int remaining = unfilledMembers.get(memberId) - 1;
        if (remaining == 0) {
            unfilledMembers.remove(memberId);
        } else {
            unfilledMembers.put(memberId, remaining);
        }
    }

    /**
     * Determines which members can be assigned additional partitions.
     *
     * @return A map of member IDs and their capacity for additional partitions.
     */
    private Map<String, Integer> getUnfilledMembers() {
        Map<String, Integer> unfilledMembers = new HashMap<>();
        for (Map.Entry<String, Integer> potentiallyUnfilledMemberEntry : potentiallyUnfilledMembers.entrySet()) {
            String memberId = potentiallyUnfilledMemberEntry.getKey();
            Integer remaining = potentiallyUnfilledMemberEntry.getValue();
            if (expectedNumMembersWithExtraPartition > 0) {
                remaining++;
                expectedNumMembersWithExtraPartition--;
            }
            // If remaining is still 0 because there were no more members required to get an extra partition,
            // we don't add it to the unfilled list.
            if (remaining > 0) {
                unfilledMembers.put(memberId, remaining);
            }
        }
        return unfilledMembers;
    }

    /**
     * This method compares the full list of partitions against the set of already
     * assigned partitions to identify those that still need to be allocated.
     *
     * @param allAssignedStickyPartitions   Set of partitions that have already been assigned.
     * @return List of unassigned partitions.
     */
    private List<TopicIdPartition> getUnassignedPartitions(Set<TopicIdPartition> allAssignedStickyPartitions) {
        List<TopicIdPartition> unassignedPartitions = new ArrayList<>();
        List<Uuid> sortedAllTopics = new ArrayList<>(subscriptionList);
        Collections.sort(sortedAllTopics);

        if (allAssignedStickyPartitions.isEmpty()) {
            return getAllTopicPartitions(sortedAllTopics, subscribedTopicDescriber);
        }

        for (Uuid topic : sortedAllTopics) {
            int partitionCount = subscribedTopicDescriber.numPartitions(topic);

            for (int i = 0; i < partitionCount; i++) {
                TopicIdPartition partition = new TopicIdPartition(topic, i);
                if (!allAssignedStickyPartitions.contains(partition)) {
                    unassignedPartitions.add(partition);
                }
            }
        }

        return unassignedPartitions;
    }

    private boolean ensureTotalUnassignedPartitionsEqualsTotalRemainingAssignments() {
        int totalRemaining = 0;
        for (Map.Entry<String, Integer> unfilledEntry  : unfilledMembers.entrySet()) {
            totalRemaining += unfilledEntry.getValue();
        }
        return totalRemaining == unassignedPartitions.size();
    }
}
