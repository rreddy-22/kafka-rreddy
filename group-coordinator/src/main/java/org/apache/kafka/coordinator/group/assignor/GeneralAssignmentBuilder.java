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

        sortedMembersPerTopic = getSortedMembersPerTopic(membersPerTopic(metadataPerMember));

        validPreviousAssignment = getValidPreviousAssignment(metadataPerMember);

        newAssignment = new HashMap<>();
        metadataPerMember.forEach((memberId, assignmentMemberSpec) -> newAssignment.put(memberId, new HashMap<>()));

        firstRoundAssignment();

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

    private void firstRoundAssignment() {
        sortedMembersPerTopic.forEach((topicId, subscribedMembers) -> {
            int numPartitionsForTopic = metadataPerTopic.get(topicId).numPartitions();
            int numSubscribedMembers = subscribedMembers.size();

            Map<String, Integer> sortedSubscribedMembersWithTotalAssignmentSize = getSortedSubscribedMembersWithTotalAssignmentSize(subscribedMembers);
            System.out.println("Sorted subscribed members list for topic id " + topicId + "is " + sortedSubscribedMembersWithTotalAssignmentSize);

            // Sum of total number of partitions assigned to all subscribed members so far.
            int totalPartitionsCurrentlyAssigned = sortedSubscribedMembersWithTotalAssignmentSize.values().stream().mapToInt(Integer::intValue).sum();
            System.out.println("New assignment " + newAssignment + "total partitions currently assigned " + totalPartitionsCurrentlyAssigned);

            int approximateQuotaPerMember = (numPartitionsForTopic + totalPartitionsCurrentlyAssigned) / numSubscribedMembers;
            int numMembersWithExtraPartition = (numPartitionsForTopic + totalPartitionsCurrentlyAssigned) % numSubscribedMembers;

            // Initially consider all partitions unassigned, sticky partitions will be removed as and when they're assigned.
            Set<Integer> unassignedPartitions = IntStream.range(0, numPartitionsForTopic).boxed().collect(Collectors.toSet());

            for (String memberId : sortedSubscribedMembersWithTotalAssignmentSize.keySet()) {
                int numPartitionsPreviouslyAssignedForTopic = validPreviousAssignment.get(memberId).getOrDefault(topicId, new HashSet<>()).size();
                if (numPartitionsPreviouslyAssignedForTopic > 0) {
                    assignStickyPartitions(approximateQuotaPerMember, numMembersWithExtraPartition, topicId, memberId, numPartitionsPreviouslyAssignedForTopic);
                    unassignedPartitions.removeAll(newAssignment.get(memberId).get(topicId));
                }
                allocateUnassignedPartitions(unassignedPartitions, sortedSubscribedMembersWithTotalAssignmentSize, approximateQuotaPerMember, numMembersWithExtraPartition, topicId);
            }
        });
    }

    private void allocateUnassignedPartitions(Set<Integer> unassignedPartitions, Map<String, Integer> sortedSubscribedMembersWithTotalAssignmentSize, int approximateQuotaPerMember, int numMembersWithExtraPartition,  Uuid topicId) {
        // Since the map doesn't guarantee order we need a list of memberIds to map each consumer to a particular index
        List<String> memberIds = new ArrayList<>(sortedSubscribedMembersWithTotalAssignmentSize.keySet());
        int[] currentIndexForMember = new int[memberIds.size()];
        List<Integer> unassignedPartitionsList = new ArrayList<>(unassignedPartitions);
        int numMembers = memberIds.size();

        for (int i = 0; i < unassignedPartitions.size(); i++) {
            int memberIndex = i % numMembers;
            int memberLimit = approximateQuotaPerMember - newAssignment.get(memberIds.get(memberIndex)).get(topicId).size();
            if (numMembersWithExtraPartition > 0) {
                memberLimit++;
                numMembersWithExtraPartition--;
            }
            // If the current consumer has reached its limit, find a consumer that has more space available in its assignment
            while (memberLimit <= 0 && currentIndexForMember[memberIndex] >= memberLimit) {
                memberIndex = (memberIndex + 1) % numMembers;
                memberLimit = approximateQuotaPerMember - newAssignment.get(memberIds.get(memberIndex)).get(topicId).size();
            }
            if (currentIndexForMember[memberIndex] < memberLimit) {
                currentIndexForMember[memberIndex]++;
                newAssignment.get(memberIds.get(memberIndex)).get(topicId).add(unassignedPartitionsList.get(i));
            }
        }
    }

    private void assignStickyPartitions(Integer approximateQuotaPerMember, Integer numMembersWithExtraPartition, Uuid topicId, String memberId, Integer numPartitionsPreviouslyAssignedForTopic) {
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
    }

    /** Sort in ascending order of total number of partitions already assigned to the member in the new assignment.
     * If size of the assignment is the same, sort in ascending order of number of topics the member is subscribed to.
    */
    private Map<String, Integer> getSortedSubscribedMembersWithTotalAssignmentSize(List<String> unsortedSubscribedMembers) {
        Map<String, Integer> sortedSubscribedMembersWithTotalAssignmentSize = new HashMap<>();
        // Populate the Map with member names as keys and totalAssignmentSize as values
        for (String memberId : unsortedSubscribedMembers) {
            int totalAssignmentSize = newAssignment.get(memberId).values().stream()
                    .flatMapToInt(assignment -> assignment.stream().mapToInt(Integer::intValue))
                    .sum();
            sortedSubscribedMembersWithTotalAssignmentSize.put(memberId, totalAssignmentSize);
        }

        // Sort the members in ascending order of total assignment size
        sortedSubscribedMembersWithTotalAssignmentSize = sortedSubscribedMembersWithTotalAssignmentSize.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.naturalOrder()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

        return sortedSubscribedMembersWithTotalAssignmentSize;
    }

    /**
     * Sort the topic Ids in ascending order based on the number of consumers subscribed to it.
     */
    private Map<Uuid, List<String>> getSortedMembersPerTopic(Map<Uuid, List<String>> unsortedMembersPerTopic) {

        List<Map.Entry<Uuid, List<String>>> unsortedMembersPerTopicList = new ArrayList<>(unsortedMembersPerTopic.entrySet());
        unsortedMembersPerTopicList.sort(Comparator.comparingInt(entry -> entry.getValue().size()));
        // Create a new LinkedHashMap to preserve the sorted order
        Map<Uuid, List<String>> sortedMap = new LinkedHashMap<>();
        for (Map.Entry<Uuid, List<String>> entry : unsortedMembersPerTopicList) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }
        return sortedMap;
    }

    private Map<Uuid, List<String>> membersPerTopic(Map<String, AssignmentMemberSpec> membersData) {
        Map<Uuid, List<String>> membersPerTopic = new HashMap<>();
        for (Map.Entry<String, AssignmentMemberSpec> memberEntry : membersData.entrySet()) {
            String memberId = memberEntry.getKey();
            AssignmentMemberSpec memberMetadata = memberEntry.getValue();
            Collection<Uuid> topics = memberMetadata.subscribedTopicIds();
            for (Uuid topicId: topics) {
                membersPerTopic.computeIfAbsent(topicId, k -> new ArrayList<>()).add(memberId);
            }
        }
        return membersPerTopic;
    }

    /**
     * Only keep partitions from the previous assignment if the topics they belong to exist in the current topic metadata and member subscriptions.
     */
    private Map<String, Map<Uuid, Set<Integer>>> getValidPreviousAssignment(Map<String, AssignmentMemberSpec> membersMetadata) {
        Map<String, Map<Uuid, Set<Integer>>> validCurrentAssignment = new HashMap<>();

        membersMetadata.forEach((memberId, assignmentMemberSpec) -> {
            Map<Uuid, Set<Integer>> validCurrentAssignmentForMember = new HashMap<>();
            assignmentMemberSpec.assignedPartitions().forEach((topicId, partitions) -> {
                if (metadataPerTopic.containsKey(topicId) && assignmentMemberSpec.subscribedTopicIds().contains(topicId)) {
                    validCurrentAssignmentForMember.put(topicId, assignmentMemberSpec.assignedPartitions().get(topicId));
                }
            });
            validCurrentAssignment.put(memberId, validCurrentAssignmentForMember);
        });
        return validCurrentAssignment;
    }

}
