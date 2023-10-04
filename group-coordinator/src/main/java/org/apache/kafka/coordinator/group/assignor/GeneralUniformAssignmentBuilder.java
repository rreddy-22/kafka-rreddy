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
import org.apache.kafka.server.common.TopicIdPartition;
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
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class GeneralUniformAssignmentBuilder extends AbstractUniformAssignmentBuilder {
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
     * The list of all the topic Ids that the consumer group is subscribed to.
     */
    private final Set<Uuid> subscriptionIds;

    /**
     * Rack information.
     */
    private final RackInfo rackInfo;

    /**
     * List of subscribed members for each topic.
     */
    private final Map<Uuid, List<String>> membersPerTopic;

    /**
     * List of all members sorted by their assignment sizes.
     */
    private final TreeSet<String> sortedMembersByAssignmentSize;

    /**
     * The partitions that still need to be assigned.
     */
    private final Set<TopicIdPartition> unassignedPartitions;

    /**
     * All the partitions that are retained from the existing assignment.
     */
    private final Set<TopicIdPartition> assignedStickyPartitions;

    /**
     * Maintains a sorted set of consumers based on how many topic partitions are already assigned to them.
     */
    private final AssignmentManager assignmentManager;

    /**
     * Tracks the owner of each partition in the existing assignment on the client side.
     *
     * Only populated with partitions that weren't retained due to a rack mismatch when rack aware strategy is used.
     */
    private final Map<TopicIdPartition, String> currentPartitionOwners;

    /**
     * Tracks the owner of each partition in the target assignment.
     */
    private final Map<TopicIdPartition, String> partitionOwnerInTargetAssignment;

    /**
     * Handles all operations related to partition movements during a reassignment for balancing the target assignment.
     */
    private PartitionMovements partitionMovements;

    /**
     * The new assignment that will be returned.
     */
    private final Map<String, MemberAssignment> targetAssignment;

    public GeneralUniformAssignmentBuilder(AssignmentSpec assignmentSpec, SubscribedTopicDescriber subscribedTopicDescriber) {
        this.members = assignmentSpec.members();
        this.subscribedTopicDescriber = subscribedTopicDescriber;
        this.subscriptionIds = assignmentSpec.members().values().stream()
            .flatMap(memberSpec -> memberSpec.subscribedTopicIds().stream())
            .peek(topicId -> {
                int partitionCount = subscribedTopicDescriber.numPartitions(topicId);
                if (partitionCount == -1) {
                    throw new PartitionAssignorException(
                        "Members are subscribed to topic " + topicId + " which doesn't exist in the topic metadata."
                    );
                }
            })
            .collect(Collectors.toSet());
        this.rackInfo = new RackInfo(assignmentSpec, subscribedTopicDescriber, subscriptionIds);
        this.membersPerTopic = new HashMap<>();
        members.forEach((memberId, memberMetadata) -> {
            Collection<Uuid> topics = memberMetadata.subscribedTopicIds();
            topics.forEach(topicId ->
                membersPerTopic.computeIfAbsent(topicId, k -> new ArrayList<>()).add(memberId)
            );
        });
        this.unassignedPartitions = new HashSet<>(allTopicIdPartitions(subscriptionIds, subscribedTopicDescriber));
        this.assignedStickyPartitions = new HashSet<>();
        this.assignmentManager = new AssignmentManager();
        this.sortedMembersByAssignmentSize = assignmentManager.getSortedMembersByAssignmentSize(members.keySet());
        this.currentPartitionOwners = new HashMap<>();
        this.partitionOwnerInTargetAssignment = new HashMap<>();
        this.targetAssignment = new HashMap<>();
    }

    @Override
    protected GroupAssignment buildAssignment() {
        if (subscriptionIds.isEmpty()) {
            LOG.info("The subscription list is empty, returning an empty assignment");
            return new GroupAssignment(Collections.emptyMap());
        }

        members.keySet().forEach(memberId -> targetAssignment.put(memberId, new MemberAssignment(new HashMap<>())));
        partitionMovements = new PartitionMovements();

        // When rack awareness is enabled, only sticky partitions with matching rack are retained.
        // Otherwise, all existing partitions are retained until max assignment size.
        assignStickyPartitions();
        if (rackInfo.useRackStrategy) rackAwarePartitionAssignment();
        unassignedPartitionsAssignment();

        balance();

        return new GroupAssignment(targetAssignment);
    }

    /**
     * Topic Ids are sorted in descending order based on the value:
     * totalPartitions/number of subscribed members.
     * If the above value is the same then topic Ids are sorted in ascending order of number of subscribers.
     * If both criteria are the same, sort in ascending order of partition Id. This is just for convenience.
     *
     * @param topicIdPartitions       The topic partitions that need to be sorted.
     * @return A list of sorted topic partitions.
     */
    // Package private for testing.
    private List<TopicIdPartition> sortTopicIdPartitions(Collection<TopicIdPartition> topicIdPartitions) {
        Comparator<TopicIdPartition> comparator = Comparator
            .comparingDouble((TopicIdPartition topicIdPartition) -> {
                int totalPartitions = subscribedTopicDescriber.numPartitions(topicIdPartition.topicId());
                int totalSubscribers = membersPerTopic.get(topicIdPartition.topicId()).size();
                return (double) totalPartitions / totalSubscribers;
            })
            .reversed()
            .thenComparingInt(topicIdPartition -> membersPerTopic.get(topicIdPartition.topicId()).size())
            .thenComparingInt(TopicIdPartition::partitionId);

        return topicIdPartitions.stream()
            .sorted(comparator)
            .collect(Collectors.toList());
    }

    /**
     * Gets a set of partitions that are retained from the existing assignment. This includes:
     * - Partitions from topics present in both the new subscriptions and the topic metadata.
     * - If using a rack-aware strategy, only partitions where members are in the same rack are retained.
     * - Track current partition owners when there is a rack mismatch.
     */
    private void assignStickyPartitions() {
        members.forEach((memberId, assignmentMemberSpec) ->
            assignmentMemberSpec.assignedPartitions().forEach((topicId, currentAssignment) -> {
                if (assignmentMemberSpec.subscribedTopicIds().contains(topicId)) {
                    currentAssignment.forEach(partition -> {
                        TopicIdPartition topicIdPartition = new TopicIdPartition(topicId, partition);
                        if (rackInfo.useRackStrategy && rackInfo.racksMismatch(memberId, topicIdPartition)) {
                            currentPartitionOwners.put(topicIdPartition, memberId);
                        } else {
                            assignmentManager.maybeAssignPartitionToMember(topicIdPartition, memberId);
                            assignedStickyPartitions.add(topicIdPartition);
                        }
                    });
                } else {
                    LOG.debug("The topic " + topicId + " is no longer present in the subscribed topics list");
                }
            })
        );
    }

    private void rackAwarePartitionAssignment() {
        // Sort partitions in ascending order by the number of potential members with matching racks.
        // Only partitions with potential members in the same rack are returned.
        List<TopicIdPartition> sortedPartitions = rackInfo.sortPartitionsByRackMembers(unassignedPartitions);

        sortedPartitions.forEach(partition -> {
            List<String> sortedMembersByAssignmentSize = rackInfo.getSortedMembersWithMatchingRack(
                partition,
                targetAssignment
            );

            for (String memberId : sortedMembersByAssignmentSize) {
                if (assignmentManager.maybeAssignPartitionToMember(partition, memberId))
                    break;
            }
        });
    }

    private void unassignedPartitionsAssignment() {
        List<TopicIdPartition> sortedPartitions = sortTopicIdPartitions(unassignedPartitions);

        for (TopicIdPartition partition : sortedPartitions) {
            // If there was an assignment that wasn't retained due to a rack mismatch,
            // check if the sticky partition can be assigned.
            if (rackInfo.useRackStrategy && currentPartitionOwners.containsKey(partition)) {
                String prevOwner = currentPartitionOwners.get(partition);
                if  (assignmentManager.maybeAssignPartitionToMember(partition, prevOwner)) {
                    continue;
                }
            }

            TreeSet<String> sortedMembers = assignmentManager.getSortedMembersByAssignmentSize(
                membersPerTopic.get(partition.topicId()));

            for (String member : sortedMembers) {
                if  (assignmentManager.maybeAssignPartitionToMember(partition, member)) {
                    break;
                }
            }
        }
    }

    /**
     * If a topic has two or more potential consumers it is subject to reassignment.
     *
     * @return true if the topic can participate in reassignment, false otherwise.
     */
    private boolean canTopicParticipateInReassignment(Uuid topicId) {
        return membersPerTopic.get(topicId).size() >= 2;
    }

    /**
     * If a member is not assigned all its potential partitions it is subject to reassignment.
     * If any of the partitions assigned to a member is subject to reassignment the member itself
     * is subject to reassignment.
     *
     * @return true if the member can participate in reassignment, false otherwise.
     */
    private boolean canMemberParticipateInReassignment(String memberId) {
        Set<Uuid> assignedTopicIds = targetAssignment.get(memberId).targetPartitions().keySet();

        int currentAssignmentSize = assignmentManager.targetAssignmentSize(memberId);
        int maxAssignmentSize = assignmentManager.maxAssignmentSize(memberId);

        if (currentAssignmentSize > maxAssignmentSize)
            LOG.error("The member {} is assigned more partitions than the maximum possible.", memberId);

        if (currentAssignmentSize < maxAssignmentSize)
            return true;

        for (Uuid topicId: assignedTopicIds) {
            if (canTopicParticipateInReassignment(topicId))
                return true;
        }
        return false;
    }

    /**
     * Determine if the current assignment is a balanced one.
     *
     * @return true if the given assignment is balanced; false otherwise
     */
    private boolean isBalanced() {
        int min = assignmentManager.targetAssignmentSize(sortedMembersByAssignmentSize.first());
        int max = assignmentManager.targetAssignmentSize(sortedMembersByAssignmentSize
            .last());

        // If minimum and maximum numbers of partitions assigned to consumers differ by at most one return true.
        if (min >= max - 1)
            return true;

        // For each member that does not have all the topic partitions it can get make sure none of the
        // topic partitions it could but did not get cannot be moved to it (because that would break the balance).
        // Members with the least assignment sizes are checked first to see if they can receive any more partitions.
        for (String member: sortedMembersByAssignmentSize) {
            int memberPartitionCount = assignmentManager.targetAssignmentSize(member);

            // Skip if this member already has all the topic partitions it can get.
            List<Uuid> allSubscribedTopics = new ArrayList<>(members.get(member).subscribedTopicIds());
            int maxAssignmentSize = assignmentManager.maxAssignmentSize(member);

            if (memberPartitionCount == maxAssignmentSize)
                continue;

            // Otherwise make sure it cannot get any more partitions.
            for (Uuid topicId: allSubscribedTopics) {
                Set<Integer> assignedPartitions = targetAssignment.get(member).targetPartitions().get(topicId);
                for (int i = 0; i < subscribedTopicDescriber.numPartitions(topicId); i++) {
                    TopicIdPartition topicIdPartition = new TopicIdPartition(topicId, i);
                    if (assignedPartitions == null || !assignedPartitions.contains(i)) {
                        String otherMember = partitionOwnerInTargetAssignment.get(topicIdPartition);
                        int otherMemberPartitionCount = assignmentManager.targetAssignmentSize(otherMember);
                        if (memberPartitionCount + 1 < otherMemberPartitionCount) {
                            LOG.debug("{} can be moved from member {} to member {} for a more balanced assignment.",
                                topicIdPartition, otherMember, member);
                            return false;
                        }
                    }
                }
            }
        }
        return true;
    }

    /**
     * Balance the current assignment using the data structures created in the assignPartitions(...) method above.
     */
    private void balance() {
        if (!unassignedPartitions.isEmpty()) unassignedPartitionsAssignment();
        // Refill unassigned partitions will all the topicId partitions.
        unassignedPartitions.addAll(allTopicIdPartitions(subscriptionIds, subscribedTopicDescriber));

        // Narrow down the reassignment scope to only those partitions that can actually be reassigned.
        Set<TopicIdPartition> fixedPartitions = new HashSet<>();
        for (Uuid topicId: subscriptionIds) {
            if (!canTopicParticipateInReassignment(topicId)) {
                for (int i = 0; i < subscribedTopicDescriber.numPartitions(topicId); i++) {
                    fixedPartitions.add(new TopicIdPartition(topicId, i));
                }
            }
        }
        unassignedPartitions.removeAll(fixedPartitions);

        // Narrow down the reassignment scope to only those members that are subject to reassignment.
        for (String member: members.keySet()) {
            if (!canMemberParticipateInReassignment(member)) {
                sortedMembersByAssignmentSize.remove(member);
            }
        }

        // If all the partitions are fixed i.e. unassigned partitions is empty there is no point of re-balancing.
        if (!unassignedPartitions.isEmpty() && !isBalanced()) performReassignments();
    }

    private void performReassignments() {
        boolean modified;
        boolean reassignmentOccurred;
        // Repeat reassignment until no partition can be moved to improve the balance.
        do {
            // Before re-starting the round of reassignments check if the assignment is already balanced.
            if (isBalanced()) break;

            modified = false;
            reassignmentOccurred = false;
            // Reassign all reassignable partitions sorted in descending order
            // by totalPartitions/number of subscribed members,
            // until the full list is processed or a balance is achieved.
            List<TopicIdPartition> reassignablePartitions = sortTopicIdPartitions(unassignedPartitions);

            for (TopicIdPartition reassignablePartition : reassignablePartitions) {
                // Only check if there is any change in balance if any moves were made.
                if (reassignmentOccurred && isBalanced()) {
                    break;
                }
                reassignmentOccurred = false;

                // The topicIdPartition must have at least two consumers.
                if (membersPerTopic.get(reassignablePartition.topicId()).size() <= 1)
                    LOG.error("Expected more than one potential member for topicIdPartition '{}'", reassignablePartition);

                // The topicIdPartition must have a current member.
                String member = partitionOwnerInTargetAssignment.get(reassignablePartition);
                if (member == null)
                    LOG.error("Expected topicIdPartition '{}' to be assigned to a member", reassignablePartition);

                boolean foundMatchingRackMember = false;

                // If rack strategy is used and the current assignment adheres to rack matching,
                // check if another member in the same rack is better suited for this topicIdPartition.
                if (rackInfo.useRackStrategy) {

                    String memberRack = rackInfo.memberRack(member);
                    Set<String> partitionRacks = rackInfo.partitionRacks(reassignablePartition);

                    if (partitionRacks.contains(memberRack)) {
                        for (String otherMember : rackInfo.getSortedMembersWithMatchingRack(reassignablePartition, targetAssignment)) {
                            // Only subscribed members eligible for re-balancing should be considered.
                            if (!sortedMembersByAssignmentSize.contains(otherMember) ||
                                !membersPerTopic.containsKey(reassignablePartition.topicId())
                            ) continue;

                            String otherMemberRack = rackInfo.memberRack(otherMember);
                            if (otherMemberRack == null || !partitionRacks.contains(otherMemberRack))
                                continue;
                            if (assignmentManager.targetAssignmentSize(member) >
                                assignmentManager.targetAssignmentSize(otherMember) + 1
                            ) {
                                reassignPartition(reassignablePartition, otherMember);
                                modified = true;
                                reassignmentOccurred = true;
                                foundMatchingRackMember = true;
                                break;
                            }
                        }
                    }
                }

                // If rack-aware strategy is not used OR no other member with matching rack was better suited,
                // First check if the topicIdPartition already belongs to its previous owner, if not and a previous owner
                // exists, check if the topicIdPartition can be assigned to it.
                // If not, assign to any other better suited member with the topic subscription.
                if (!foundMatchingRackMember) {
                    boolean isPartitionSticky = assignedStickyPartitions.contains(reassignablePartition);
                    boolean isCurrentOwnerKnown = currentPartitionOwners.containsKey(reassignablePartition);

                    if (rackInfo.useRackStrategy && !isPartitionSticky && isCurrentOwnerKnown) {
                        String currentOwner = currentPartitionOwners.get(reassignablePartition);
                        int currentMemberAssignmentSize = assignmentManager.targetAssignmentSize(member);
                        int currentOwnerAssignmentSize = assignmentManager.targetAssignmentSize(currentOwner);

                        // Check if reassignment is needed based on assignment sizes
                        if (currentMemberAssignmentSize > currentOwnerAssignmentSize + 1) {
                            reassignPartition(reassignablePartition, currentOwner);
                            modified = true;
                            reassignmentOccurred = true;
                        }
                    }

                    // The topicIdPartition is already sticky and no other member with matching rack is better suited.
                    if (!reassignmentOccurred) {
                        for (String otherMember : membersPerTopic.get(reassignablePartition.topicId())) {
                            if (assignmentManager.targetAssignmentSize(member) >
                                assignmentManager.targetAssignmentSize(otherMember) + 1
                            ) {
                                reassignPartition(reassignablePartition);
                                modified = true;
                                reassignmentOccurred = true;
                                break;
                            }
                        }
                    }
                }
            }
        } while (modified);
    }

    private void reassignPartition(TopicIdPartition partition) {
        // Find the new member with the least assignment size.
        String newOwner = null;
        for (String anotherMember: sortedMembersByAssignmentSize) {
            if (members.get(anotherMember).subscribedTopicIds().contains(partition.topicId())) {
                newOwner = anotherMember;
                break;
            }
        }

        assert newOwner != null;

        reassignPartition(partition, newOwner);
    }

    private void reassignPartition(TopicIdPartition partition, String newMember) {
        String member = partitionOwnerInTargetAssignment.get(partition);
        // Find the correct partition movement considering the stickiness requirement.
        TopicIdPartition partitionToBeMoved = partitionMovements.getTheActualPartitionToBeMoved(partition, member, newMember);
        processPartitionMovement(partitionToBeMoved, newMember);
    }

    private void processPartitionMovement(TopicIdPartition topicIdPartition, String newMember) {
        String oldMember = partitionOwnerInTargetAssignment.get(topicIdPartition);

        partitionMovements.movePartition(topicIdPartition, oldMember, newMember);

        assignmentManager.removePartitionFromMembersAssignment(topicIdPartition, oldMember);
        assignmentManager.addPartitionToMembersAssignment(topicIdPartition, newMember);
    }

    /**
     * <code> MemberPair </code> represents a pair of Kafka consumer ids involved in a partition reassignment. Each
     * <code> MemberPair </code> object, which contains a source (<code>src</code>) and a destination (<code>dst</code>)
     * element, normally corresponds to a particular partition or topic, and indicates that the particular partition or some
     * partition of the particular topic was moved from the source consumer to the destination consumer during the rebalance.
     * This class is used, through the <code>PartitionMovements</code> class, by the sticky assignor and helps in determining
     * whether a partition reassignment results in cycles among the generated graph of consumer pairs.
     */
    private static class MemberPair {
        private final String srcMemberId;
        private final String dstMemberId;

        MemberPair(String srcMemberId, String dstMemberId) {
            this.srcMemberId = srcMemberId;
            this.dstMemberId = dstMemberId;
        }

        public String toString() {
            return this.srcMemberId + "->" + this.dstMemberId;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((this.srcMemberId == null) ? 0 : this.srcMemberId.hashCode());
            result = prime * result + ((this.dstMemberId == null) ? 0 : this.dstMemberId.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null)
                return false;

            if (!getClass().isInstance(obj))
                return false;

            MemberPair otherPair = (MemberPair) obj;
            return this.srcMemberId.equals(otherPair.srcMemberId) && this.dstMemberId.equals(otherPair.dstMemberId);
        }
    }

    /**
     * This class maintains some data structures to simplify lookup of partition movements among consumers. At each point of
     * time during a partition rebalance it keeps track of partition movements corresponding to each topic, and also possible
     * movement (in form a <code>MemberPair</code> object) for each partition.
     */
    private static class PartitionMovements {
        private final Map<Uuid, Map<MemberPair, Set<TopicIdPartition>>> partitionMovementsByTopic = new HashMap<>();
        private final Map<TopicIdPartition, MemberPair> partitionMovementsByPartition = new HashMap<>();

        private MemberPair removeMovementRecordOfPartition(TopicIdPartition partition) {
            MemberPair pair = partitionMovementsByPartition.remove(partition);

            Uuid topic = partition.topicId();
            Map<MemberPair, Set<TopicIdPartition>> partitionMovementsForThisTopic = partitionMovementsByTopic.get(topic);
            partitionMovementsForThisTopic.get(pair).remove(partition);
            if (partitionMovementsForThisTopic.get(pair).isEmpty())
                partitionMovementsForThisTopic.remove(pair);
            if (partitionMovementsByTopic.get(topic).isEmpty())
                partitionMovementsByTopic.remove(topic);

            return pair;
        }

        private void addPartitionMovementRecord(TopicIdPartition partition, MemberPair pair) {
            partitionMovementsByPartition.put(partition, pair);

            Uuid topic = partition.topicId();
            if (!partitionMovementsByTopic.containsKey(topic))
                partitionMovementsByTopic.put(topic, new HashMap<>());

            Map<MemberPair, Set<TopicIdPartition>> partitionMovementsForThisTopic = partitionMovementsByTopic.get(topic);
            if (!partitionMovementsForThisTopic.containsKey(pair))
                partitionMovementsForThisTopic.put(pair, new HashSet<>());

            partitionMovementsForThisTopic.get(pair).add(partition);
        }

        private void movePartition(TopicIdPartition partition, String oldOwner, String newOwner) {
            MemberPair pair = new MemberPair(oldOwner, newOwner);

            if (partitionMovementsByPartition.containsKey(partition)) {
                // This partition was previously moved.
                MemberPair existingPair = removeMovementRecordOfPartition(partition);
                assert existingPair.dstMemberId.equals(oldOwner);
                if (!existingPair.srcMemberId.equals(newOwner)) {
                    // The partition is not moving back to its previous consumer.
                    addPartitionMovementRecord(partition, new MemberPair(existingPair.srcMemberId, newOwner));
                }
            } else
                addPartitionMovementRecord(partition, pair);
        }

        private TopicIdPartition getTheActualPartitionToBeMoved(TopicIdPartition partition, String oldOwner, String newOwner) {
            Uuid topic = partition.topicId();

            if (!partitionMovementsByTopic.containsKey(topic))
                return partition;

            // This partition was previously moved.
            if (partitionMovementsByPartition.containsKey(partition)) {
                assert oldOwner.equals(partitionMovementsByPartition.get(partition).dstMemberId);
                oldOwner = partitionMovementsByPartition.get(partition).srcMemberId;
            }

            Map<MemberPair, Set<TopicIdPartition>> partitionMovementsForThisTopic = partitionMovementsByTopic.get(topic);
            MemberPair reversePair = new MemberPair(newOwner, oldOwner);
            if (!partitionMovementsForThisTopic.containsKey(reversePair))
                return partition;

            return partitionMovementsForThisTopic.get(reversePair).iterator().next();
        }
    }

    /**
     * Manages assignments to members based on their current assignment size and maximum allowed assignment size.
     */
    protected class AssignmentManager {
        private final Map<String, MemberAssignmentData> membersWithAssignmentSizes = new HashMap<>();

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

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                MemberAssignmentData that = (MemberAssignmentData) o;
                return memberId.equals(that.memberId);
            }

            @Override
            public int hashCode() {
                return Objects.hash(memberId);
            }

            @Override
            public String toString() {
                return "MemberAssignmentData{" +
                    "memberId='" + memberId + '\'' +
                    ", currentAssignmentSize=" + currentAssignmentSize +
                    ", maxAssignmentSize=" + maxAssignmentSize +
                    '}';
            }
        }

        /**
         * Initializes an AssignmentManager, setting up the necessary data structures.
         */
        public AssignmentManager() {
            initializeAssignmentSizes();
        }

        /**
         * @param memberId       The member Id.
         * @return The current assignment size for the given member.
         */
        public int targetAssignmentSize(String memberId) {
            MemberAssignmentData memberData = this.membersWithAssignmentSizes.get(memberId);
            if (memberData == null) {
                LOG.warn("Member Id {} not found", memberId);
                return 0;
            }
            return memberData.currentAssignmentSize;
        }

        /**
         * @param memberId      The member Id.
         * @return The maximum assignment size for the given member.
         */
        public int maxAssignmentSize(String memberId) {
            MemberAssignmentData memberData = this.membersWithAssignmentSizes.get(memberId);
            if (memberData == null) {
                LOG.warn("Member Id {} not found", memberId);
                return 0;
            }
            return memberData.maxAssignmentSize;
        }

        /**
         * @param memberId      The member Id.
         * Increment the current target assignment size for the member.
         */
        public void incrementTargetAssignmentSize(String memberId) {
            MemberAssignmentData memberData = this.membersWithAssignmentSizes.get(memberId);
            if (memberData == null) {
                LOG.warn("Member Id {} not found", memberId);
                return;
            }
            memberData.currentAssignmentSize++;
        }

        /**
         * @param memberId      The member Id.
         * Decrement the current target assignment size for the member if it's assignment size is greater than zero.
         */
        public void decrementTargetAssignmentSize(String memberId) {
            MemberAssignmentData memberData = this.membersWithAssignmentSizes.get(memberId);
            if (memberData == null) {
                LOG.warn("Member Id {} not found", memberId);
                return;
            }

            if (memberData.currentAssignmentSize > 0) {
                memberData.currentAssignmentSize --;
            }
        }

        /**
         * Computes and sets the maximum assignment size for each member.
         */
        private void initializeAssignmentSizes() {
            members.forEach((memberId, member) -> {
                int maxSize = member.subscribedTopicIds().stream()
                    .mapToInt(subscribedTopicDescriber::numPartitions)
                    .sum();

                MemberAssignmentData memberAssignmentData = membersWithAssignmentSizes
                    .computeIfAbsent(memberId, MemberAssignmentData::new);
                memberAssignmentData.maxAssignmentSize = maxSize;
                memberAssignmentData.currentAssignmentSize = 0;
            });
        }

        /**
         * Assigns partition to member if eligible.
         *
         * @param topicIdPartition      The partition to be assigned.
         * @param memberId              The Id of the member.
         * @return true if the partition was assigned, false otherwise.
         */
        protected boolean maybeAssignPartitionToMember(
            TopicIdPartition topicIdPartition,
            String memberId
        ) {
            // If member is not subscribed to the partition's topic, return false without assigning.
            if (!members.get(memberId).subscribedTopicIds().contains(topicIdPartition.topicId())) {
                return false;
            }

            // If the member's current assignment is already at max, return false without assigning.
            if (assignmentManager.targetAssignmentSize(memberId) >= assignmentManager.maxAssignmentSize(memberId)) {
                return false;
            }

            addPartitionToMembersAssignment(topicIdPartition, memberId);
            return true;
        }

        /**
         * Assigns a partition to a member (if eligible), updates the current assignment size,
         * and updates relevant data structures.
         *
         * @param topicIdPartition      The partition to be assigned.
         * @param memberId              Member that the partition needs to be added to.
         */
        private void addPartitionToMembersAssignment(TopicIdPartition topicIdPartition, String memberId) {
            addPartitionToAssignment(
                targetAssignment,
                memberId,
                topicIdPartition.topicId(),
                topicIdPartition.partitionId()
            );

            partitionOwnerInTargetAssignment.put(topicIdPartition, memberId);
            // Remove the member's assignment data from the queue to update it.
            sortedMembersByAssignmentSize.remove(memberId);
            assignmentManager.incrementTargetAssignmentSize(memberId);

            // Update current assignment size and re-add to queue if needed.
            if (assignmentManager.targetAssignmentSize(memberId) < assignmentManager.maxAssignmentSize(memberId)) {
                sortedMembersByAssignmentSize.add(memberId);
            }

            unassignedPartitions.remove(topicIdPartition);
        }

        /**
         * Revokes the partition from a member, updates the current target assignment size,
         * and other relevant data structures.
         *
         * @param topicIdPartition      The partition to be revoked.
         * @param memberId              Member that the partition needs to be revoked from.
         */
        private void removePartitionFromMembersAssignment(TopicIdPartition topicIdPartition, String memberId) {
            Map<Uuid, Set<Integer>> targetPartitionsMap = targetAssignment.get(memberId).targetPartitions();
            Set<Integer> partitionsSet = targetPartitionsMap.get(topicIdPartition.topicId());
            // Remove the partition from the assignment, if there are no more partitions from a particular topic,
            // remove the topic from the assignment as well.
            if (partitionsSet != null) {
                partitionsSet.remove(topicIdPartition.partitionId());
                if (partitionsSet.isEmpty()) {
                    targetPartitionsMap.remove(topicIdPartition.topicId());
                }
            }

            partitionOwnerInTargetAssignment.remove(topicIdPartition, memberId);
            // Remove the member's assignment data from the queue to update it.
            sortedMembersByAssignmentSize.remove(memberId);
            assignmentManager.decrementTargetAssignmentSize(memberId);

            // Update current assignment size and re-add to queue if needed.
            if (assignmentManager.targetAssignmentSize(memberId) < assignmentManager.maxAssignmentSize(memberId)) {
                sortedMembersByAssignmentSize.add(memberId);
            }
        }

        /**
         * Sorts members in ascending or descending order based on their current target assignment size.
         * Members that have reached their max assignment size are removed.
         *
         * @param memberIds     All the member Ids that need to be sorted.
         * @return A set that maintains the order of members by assignment size.
         */
        protected TreeSet<String> getSortedMembersByAssignmentSize(Collection<String> memberIds) {
            Comparator<String> comparator = Comparator
                .comparingInt((String memberId) -> membersWithAssignmentSizes.get(memberId).currentAssignmentSize)
                .thenComparing(memberId -> memberId);

            return memberIds.stream()
                .filter(memberId -> {
                    MemberAssignmentData memberData = membersWithAssignmentSizes.get(memberId);
                    return memberData.currentAssignmentSize < memberData.maxAssignmentSize;
                })
                .collect(Collectors.toCollection(() -> new TreeSet<>(comparator)));
        }
    }
}
