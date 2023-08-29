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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class GeneralAssignmentBuilder extends UniformAssignor.AbstractAssignmentBuilder {

    private static final Logger log = LoggerFactory.getLogger(GeneralAssignmentBuilder.class);
    // Topics are sorted in ascending order based on how many consumers are subscribed to it.
    private Map<Uuid, List<String>> sortedMembersPerTopic;
    // Partitions from the existing assignment that are still viable to be in the current assignment.
    private Map<String, Map<Uuid, Set<Integer>>> validPreviousAssignment;

    private Map<String, Map<Uuid, Set<Integer>>> newAssignment;

    GeneralAssignmentBuilder(AssignmentSpec assignmentSpec) {
        super(assignmentSpec);
    }

    @Override
    GroupAssignment build() {

        sortedMembersPerTopic = getSortedMembersPerTopic(metadataPerMember);
        System.out.println("Sorted members per topic" + sortedMembersPerTopic);
        validPreviousAssignment = getValidPreviousAssignment(metadataPerMember);

        newAssignment = new HashMap<>();
        metadataPerMember.forEach((memberId, assignmentMemberSpec) -> newAssignment.put(memberId, new HashMap<>()));

        assignPartitions();
        System.out.println("Assignment after retaining sticky partitions " + newAssignment);

        // Consolidate the maps into MemberAssignment and then finally map each consumer to a MemberAssignment.
        Map<String, MemberAssignment> membersWithNewAssignment = new HashMap<>();
        for (Map.Entry<String, Map<Uuid, Set<Integer>>> consumer : newAssignment.entrySet()) {
            String consumerId = consumer.getKey();
            Map<Uuid, Set<Integer>> assignmentPerTopic = consumer.getValue();
            membersWithNewAssignment.computeIfAbsent(consumerId, k -> new MemberAssignment(assignmentPerTopic));
        }

        GroupAssignment newGroupAssignment = new GroupAssignment(membersWithNewAssignment);

        System.out.println("The full assignment is " + newGroupAssignment);
        // Add a check balance
        return newGroupAssignment;
    }

    private void assignPartitions() {

        sortedMembersPerTopic.forEach((topicId, subscribedMembers) -> {
            System.out.println("RETAIN STICKY PART");
            int numPartitionsForTopic = metadataPerTopic.get(topicId).numPartitions();
            int numSubscribedMembers = subscribedMembers.size();
            Map<String, Integer> sortedSubscribedMembersWithTotalAssignmentSize = getSortedSubscribedMembersWithTotalAssignmentSize(subscribedMembers);
            System.out.println("Sorted subscribed members by total current assignment size for topic id " + topicId + "is " + sortedSubscribedMembersWithTotalAssignmentSize);
            // Sum of total number of partitions assigned to all subscribed members so far.
            int totalPartitionsCurrentlyAssigned = sortedSubscribedMembersWithTotalAssignmentSize.values().stream().mapToInt(Integer::intValue).sum();
            System.out.println("New assignment " + newAssignment + "total partitions currently assigned " + totalPartitionsCurrentlyAssigned);

            int approximateQuotaPerMember = (numPartitionsForTopic + totalPartitionsCurrentlyAssigned) / numSubscribedMembers;
            int numMembersWithExtraPartition = (numPartitionsForTopic + totalPartitionsCurrentlyAssigned) % numSubscribedMembers;
            System.out.println("Approximate quota per member " + approximateQuotaPerMember + " number of members with extra partition " + numMembersWithExtraPartition);

            // Initially consider all partitions unassigned, sticky partitions will be removed as and when they're assigned.
            Set<Integer> availablePartitions = IntStream.range(0, numPartitionsForTopic).boxed().collect(Collectors.toSet());
            System.out.println(" Available partitions initially" + availablePartitions);

            for (String memberId : sortedSubscribedMembersWithTotalAssignmentSize.keySet()) {
                int numPartitionsPreviouslyAssignedForTopic = validPreviousAssignment.get(memberId).getOrDefault(topicId, new HashSet<>()).size();
                System.out.println("valid prev assignment for member" + memberId + "is " + validPreviousAssignment.get(memberId));
                System.out.println(" number of partitions previously assigned for topic " + topicId + " partitions = " + numPartitionsPreviouslyAssignedForTopic);
                // Allocate assigned sticky partitions
                if (numPartitionsPreviouslyAssignedForTopic > 0) {
                    if (numPartitionsPreviouslyAssignedForTopic <= approximateQuotaPerMember) {
                        Set<Integer> partitions = validPreviousAssignment.get(memberId).get(topicId);
                        newAssignment.get(memberId).put(topicId, partitions);
                    } else {
                        int extra = 0;
                        if (numMembersWithExtraPartition > 0) {
                            extra += 1;
                            numMembersWithExtraPartition--;
                        }
                        Set<Integer> partitions = validPreviousAssignment.get(memberId).get(topicId).stream().limit(approximateQuotaPerMember + extra).collect(Collectors.toSet());
                        newAssignment.get(memberId).put(topicId, partitions);
                    }
                    availablePartitions.removeAll(newAssignment.get(memberId).get(topicId));
                }
                System.out.println(" new assignment after sticky ones assigned for member" + memberId + "is" + newAssignment);
            }
            System.out.println("ASSIGN UNASSIGNED PART");
            sortedSubscribedMembersWithTotalAssignmentSize = getSortedSubscribedMembersWithTotalAssignmentSize(subscribedMembers);
            System.out.println("Sorted subscribed members by total current assignment size for topic id " + topicId + "is " + sortedSubscribedMembersWithTotalAssignmentSize);
            // Allocate unassigned partitions
            // Since the map doesn't guarantee order we need a list of memberIds to map each consumer to a particular index.
            List<String> memberIds = new ArrayList<>(sortedSubscribedMembersWithTotalAssignmentSize.keySet());
            int[] currentIndexForMember = new int[memberIds.size()];
            int[] memberLimits = new int[memberIds.size()];
            List<Integer> unassignedPartitionsList = new ArrayList<>(availablePartitions);
            int numMembers = memberIds.size();

            for (int i = 0; i < memberIds.size(); i++) {
                memberLimits[i] = approximateQuotaPerMember - sortedSubscribedMembersWithTotalAssignmentSize.get(memberIds.get(i));
                if (numMembersWithExtraPartition > 0) {
                    memberLimits[i]++;
                    numMembersWithExtraPartition--;
                }
                System.out.println(" member limit for member " + memberIds.get(i) + " is " + memberLimits[i]);
            }

            for (int i = 0; i < unassignedPartitionsList.size(); i++) {
                int memberIndex = i % numMembers;
                int memberLimit = memberLimits[memberIndex];
                System.out.println("current index for member " + memberIds.get(memberIndex) + " is" + currentIndexForMember[memberIndex]);
                // If the current consumer has reached its limit, find a consumer that has more space available in its assignment
                while (memberLimit <= 0 || currentIndexForMember[memberIndex] >= memberLimit) {
                    memberIndex = (memberIndex + 1) % numMembers;
                    memberLimit = memberLimits[memberIndex];
                }
                if (currentIndexForMember[memberIndex] < memberLimit) {
                    currentIndexForMember[memberIndex]++;
                    newAssignment.get(memberIds.get(memberIndex)).computeIfAbsent(topicId, k -> new HashSet<>()).add(unassignedPartitionsList.get(i));
                }
                System.out.println("Assignment is now" + newAssignment);

            }
        });
    }

    /** Sort in ascending order of total number of partitions already assigned to the member in the new assignment.
     * If size of the assignment is the same, sort in ascending order of number of topics the member is subscribed to.
     * If the total assignment size and the number of topics the member is subscribed to is same then sort by total previous assignment size.
    */
    private Map<String, Integer> getSortedSubscribedMembersWithTotalAssignmentSize(List<String> unsortedSubscribedMembers) {
        return unsortedSubscribedMembers.stream()
            .sorted(Comparator.comparingInt((String memberId) -> newAssignment.get(memberId).values().stream()
                    .flatMapToInt(set -> IntStream.of(set.size()))
                    .sum())
                .thenComparingInt(memberId -> metadataPerMember.get(memberId).subscribedTopicIds().size())
                .thenComparingInt(memberId -> validPreviousAssignment.get(memberId).values().stream()
                    .flatMapToInt(set -> set.stream().mapToInt(Integer::intValue))
                    .sum()))
            .collect(Collectors.toMap(
                memberId -> memberId,
                memberId -> newAssignment.get(memberId).values().stream()
                    .flatMapToInt(set -> IntStream.of(set.size()))
                    .sum(),
                Integer::sum,
                LinkedHashMap::new
            ));
    }


    /**
     * Get subscribed members per topic and sort the topic Ids in descending order based on the totalPartitions/number of consumers subscribed to it.
     * If the above value is the same then sort in ascending order of number of subscribers.
     */
    private Map<Uuid, List<String>> getSortedMembersPerTopic(Map<String, AssignmentMemberSpec> membersData) {
        Map<Uuid, List<String>> membersPerTopic = new HashMap<>();
        membersData.forEach((memberId, memberMetadata) -> {
            Collection<Uuid> topics = memberMetadata.subscribedTopicIds();
            for (Uuid topicId: topics) {
                membersPerTopic.computeIfAbsent(topicId, k -> new ArrayList<>()).add(memberId);
            }
        });
        System.out.println("Members per topic " + membersPerTopic);

        Comparator<Object> backupComparator = Comparator.comparingInt(topicId ->
            metadataPerTopic.get(topicId).numPartitions()).reversed();

        Comparator<Object> comparator = Comparator.comparingDouble(topicId -> {
            int totalSubscribers = membersPerTopic.get(topicId).size();
            return totalSubscribers;
        }).thenComparing(backupComparator);
        // Custom comparator to compare topics based on totalPartitions/totalConsumers
       /* Comparator<Object> comparator = Comparator.comparingDouble(topicId -> {
            int totalPartitions = metadataPerTopic.get(topicId).numPartitions();
            int totalSubscribers = membersPerTopic.get(topicId).size();
            double ratio = (double) totalPartitions / totalSubscribers;
            return -Math.ceil(ratio);
        }).thenComparingInt(topicId ->
            membersPerTopic.get(topicId).size());
*/
        // Create a TreeMap using the custom comparator to sort the keys
        Map<Uuid, List<String>> sortedMembersPerTopic = new TreeMap<>(comparator);
        sortedMembersPerTopic.putAll(membersPerTopic);

        return sortedMembersPerTopic;
    }

    /**
     * Only keep partitions from the previous assignment if the topics they belong to exist in the current topic metadata and member subscriptions.
     */
    private Map<String, Map<Uuid, Set<Integer>>> getValidPreviousAssignment(Map<String, AssignmentMemberSpec> membersMetadata) {
        Map<String, Map<Uuid, Set<Integer>>> validPreviousAssignment = new HashMap<>();

        membersMetadata.forEach((memberId, assignmentMemberSpec) -> {
            Map<Uuid, Set<Integer>> validPreviousAssignmentForMember = new HashMap<>();
            assignmentMemberSpec.assignedPartitions().forEach((topicId, partitions) -> {
                if (metadataPerTopic.containsKey(topicId) && assignmentMemberSpec.subscribedTopicIds().contains(topicId)) {
                    validPreviousAssignmentForMember.put(topicId, assignmentMemberSpec.assignedPartitions().get(topicId));
                }
            });
            validPreviousAssignment.put(memberId, validPreviousAssignmentForMember);
        });
        System.out.println("Valid prev assignment " + validPreviousAssignment);
        return validPreviousAssignment;
    }
}

