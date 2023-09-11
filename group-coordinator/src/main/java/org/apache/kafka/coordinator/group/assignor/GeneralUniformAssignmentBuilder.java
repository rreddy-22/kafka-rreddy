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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;

public class GeneralUniformAssignmentBuilder extends UniformAssignor.AbstractAssignmentBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(GeneralUniformAssignmentBuilder.class);

    /**
     * The member metadata obtained from the assignment specification.
     */
    private final Map<String, AssignmentMemberSpec> members;

    /**
     * The topic and partition metadata describer.
     */
    private final SubscribedTopicDescriber subscribedTopicDescriber;

    /**
     * The set of all the topic Ids that the consumer group is subscribed to.
     */
    private final Set<Uuid> subscriptionIds;

    /**
     * Rack information.
     */
    private final RackInfo rackInfo;

    /**
     * The partitions that still need to be assigned.
     */
    private List<TopicIdPartition> unassignedPartitions;

    /**
     * Members that haven't reached their max assignment size.
     * Initially contains all member Ids with their maximum assignment size.
     */
    private Map<String, Integer> potentiallyUnfilledMembersWithMaxAssignmentSize;

    /**
     * All the partitions that are retained from the existing assignment.
     */
    private Set<TopicIdPartition> assignedStickyPartitions;

    /**
     * Maintains a sorted set of consumers based on how many topic partitions are already assigned to them.
     */
    private AssignmentManager assignmentManager;

    /**
     * Tracks the current owner of each partition.
     * Current refers to the existing assignment.
     */
    private final Map<TopicIdPartition, String> currentPartitionOwners;

    /**
     * The new assignment that will be returned.
     */
    private final Map<String, MemberAssignment> targetAssignment;

    public GeneralUniformAssignmentBuilder(AssignmentSpec assignmentSpec, SubscribedTopicDescriber subscribedTopicDescriber) {
        this.members = assignmentSpec.members();
        this.subscribedTopicDescriber = subscribedTopicDescriber;
        this.subscriptionIds = assignmentSpec.members().values().stream()
            .flatMap(memberSpec -> memberSpec.subscribedTopicIds().stream())
            .collect(Collectors.toSet());
        this.rackInfo = new RackInfo(assignmentSpec, subscribedTopicDescriber, subscriptionIds);
        this.unassignedPartitions = allTopicIdPartitions(subscriptionIds, subscribedTopicDescriber);
        this.potentiallyUnfilledMembersWithMaxAssignmentSize = new HashMap<>();
        this.assignedStickyPartitions = new HashSet<>();
        this.assignmentManager = new AssignmentManager();
        this.currentPartitionOwners = new HashMap<>();
        this.targetAssignment = new HashMap<>();
    }

    @Override
    protected GroupAssignment buildAssignment() {
        if (subscriptionIds.isEmpty()) {
            LOG.info("The subscription list is empty, returning an empty assignment");
            return new GroupAssignment(Collections.emptyMap());
        }

        addMaxAssignmentSizes();

        members.keySet().forEach(memberId ->
            targetAssignment.put(memberId, new MemberAssignment(new HashMap<>())
            ));

        // When rack awareness is enabled, only sticky partitions with matching rack are retained.
        // Otherwise, all existing partitions are retained until max assignment size.
        assignStickyPartitions();

        rackAwareRoundRobinAssignment();

        return new GroupAssignment(targetAssignment);
    }

    /**
     * Adds the maximum assignment size for each member in the potentially unfilled members map.
     * The maximum assignment size of a member is the sum of all the partitions belonging to the subscribed topics.
     */
    private void addMaxAssignmentSizes() {
        members.forEach((memberId, member) ->
            potentiallyUnfilledMembersWithMaxAssignmentSize.put(
                memberId,
                member.subscribedTopicIds().stream()
                    .mapToInt(subscribedTopicDescriber::numPartitions)
                    .sum()
            )
        );
    }

    /**
     * Topic Ids are sorted in descending order based on the value:
     * totalPartitions/number of subscribed members.
     * If the above value is the same then topic Ids are sorted in ascending order of number of subscribers.
     */
    // Package private for testing.
    void sortTopics(List<Uuid> allTopics) {
        Map<Uuid, List<String>> membersPerTopic = new HashMap<>();
        members.forEach((memberId, memberMetadata) -> {
            Collection<Uuid> topics = memberMetadata.subscribedTopicIds();
            topics.forEach(topicId ->
                membersPerTopic.computeIfAbsent(topicId, k -> new ArrayList<>()).add(memberId)
            );
        });

        Comparator<Uuid> comparator = Comparator.comparingDouble((Uuid topicId) -> {
            int totalPartitions = subscribedTopicDescriber.numPartitions(topicId);
            int totalSubscribers = membersPerTopic.get(topicId).size();
            return (double) totalPartitions / totalSubscribers;
        }).reversed().thenComparingInt(topicId -> membersPerTopic.get(topicId).size());

        allTopics.stream()
            .sorted(comparator)
            .collect(Collectors.toList());
    }

    /**
     * Gets a set of partitions that are retained from the existing assignment. This includes:
     * - Partitions from topics present in both the new subscriptions and the topic metadata.
     * - If using a rack-aware strategy, only partitions where members are in the same rack are retained.
     *
     * @return A set containing all the sticky partitions that have been retained in the new assignment.
     */
    private void assignStickyPartitions() {
        members.forEach((memberId, assignmentMemberSpec) ->
            assignmentMemberSpec.assignedPartitions().forEach((topicId, currentAssignment) -> {
                if (subscriptionIds.contains(topicId)) {
                    currentAssignment.forEach(partition -> {
                        TopicIdPartition topicIdPartition = new TopicIdPartition(topicId, partition);
                        if (rackInfo.useRackStrategy && rackInfo.racksMismatch(memberId, topicIdPartition)) {
                            currentPartitionOwners.put(topicIdPartition, memberId);
                        } else {
                            assignmentManager.assignPartitionToMember(memberId, topicIdPartition);
                            assignedStickyPartitions.add(topicIdPartition);
                        }
                    });
                } else {
                    LOG.debug("The topic " + topicId + " is no longer present in the subscribed topics list");
                }
            })
        );
    }

    private void rackAwareRoundRobinAssignment() {
        // Sort partitions in ascending order by number of potential members with matching racks.
        // Partitions with no potential members in the same rack aren't included in this list.
        List<TopicIdPartition> sortedPartitions = rackInfo.sortPartitionsByRackMembers(unassignedPartitions);

        unassignedPartitions.forEach(partition -> {
            boolean assigned = false;
            for (int i = 0; i < assignmentManager.sortedMembersByAssignmentSize.size() && !assigned; i++) {
                String memberId = assignmentManager.getNextMember();

                if (!rackInfo.racksMismatch(memberId, partition)) {
                    assignmentManager.assignPartitionToMember(memberId, partition);
                    assigned = true;
                }
            }
        });
    }

    /**
     * Manages assignments to members based on their current assignment size and maximum allowed assignment size.
     */
    public class AssignmentManager {

        private final Map<String, MemberAssignmentData> membersWithAssignmentSizes = new HashMap<>();
        private final PriorityQueue<MemberAssignmentData> sortedMembersByAssignmentSize;

        /**
         * Represents the assignment metadata for a member.
         */
        private class MemberAssignmentData {

            final String memberId;
            int currentAssignmentSize = 0;
            int maxAssignmentSize;

            /**
             * Constructs a MemberAssignmentData with the given member Id.
             *
             * @param memberId The Id of the member.
             */
            MemberAssignmentData(String memberId) {
                this.memberId = memberId;
            }
        }

        /**
         * Initializes an AssignmentManager, setting up the necessary data structures.
         */
        public AssignmentManager() {
            Comparator<MemberAssignmentData> comparator = Comparator.comparingInt(
                (MemberAssignmentData ma) -> ma.currentAssignmentSize
            ).thenComparing(ma -> ma.memberId);

            sortedMembersByAssignmentSize = new PriorityQueue<>(comparator);
            initializeMaxAssignmentSizes();
        }

        /**
         * Computes and sets the maximum assignment size for each member.
         */
        private void initializeMaxAssignmentSizes() {
            members.forEach((memberId, member) -> {
                int maxSize = member.subscribedTopicIds().stream()
                    .mapToInt(subscribedTopicDescriber::numPartitions)
                    .sum();

                MemberAssignmentData memberAssignmentData = membersWithAssignmentSizes
                    .computeIfAbsent(memberId, MemberAssignmentData::new);
                memberAssignmentData.maxAssignmentSize = maxSize;

                // If a member's current assignments are less than the max, they are eligible to receive more.
                if (memberAssignmentData.currentAssignmentSize < maxSize) {
                    sortedMembersByAssignmentSize.offer(memberAssignmentData);
                }
            });
        }

        /**
         * Assigns a partition to a member, updates the current assignment size,
         * and updates relevant data structures.
         *
         * @param memberId         The Id of the member.
         * @param topicIdPartition The partition to be assigned.
         */
        private void assignPartitionToMember(String memberId, TopicIdPartition topicIdPartition) {
            addPartitionToAssignment(
                topicIdPartition.partition(),
                topicIdPartition.topicId(),
                memberId,
                targetAssignment
            );

            // Fetch or initialize MemberAssignmentData
            MemberAssignmentData memberAssignmentData = membersWithAssignmentSizes
                .computeIfAbsent(memberId, MemberAssignmentData::new);

            // Remove the member's assignment data from the queue to update it.
            sortedMembersByAssignmentSize.remove(memberAssignmentData);

            memberAssignmentData.currentAssignmentSize++;
            // Update current assignment size and re-add to queue if needed.
            if (memberAssignmentData.currentAssignmentSize < memberAssignmentData.maxAssignmentSize) {
                sortedMembersByAssignmentSize.offer(memberAssignmentData);
            }

            unassignedPartitions.remove(topicIdPartition.partition());
        }

        /**
         * Fetches the next member based on the sorting order.
         *
         * @return The memberId of the next member to be considered for assignment.
         */
        public String getNextMember() {
            return sortedMembersByAssignmentSize.peek().memberId;
        }
    }
}

