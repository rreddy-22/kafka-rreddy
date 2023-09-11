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

import org.apache.kafka.coordinator.group.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
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
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * The Uniform Assignor distributes topic partitions among group members for a balanced assignment.
 * The assignor employs two different strategies based on the nature of topic
 * subscriptions across the group members:
 * <ul>
 *     <li>
 *         <b> Optimized Uniform Assignment Builder: </b> This strategy is used when all members have subscribed
 *         to the same set of topics.
 *     </li>
 *     <li>
 *         <b> General Uniform Assignment Builder: </b> This strategy is used when members have varied topic
 *         subscriptions.
 *     </li>
 * </ul>
 *
 * The appropriate strategy is automatically chosen based on the current members' topic subscriptions.
 *
 * @see OptimizedUniformAssignmentBuilder
 * @see GeneralUniformAssignmentBuilder
 */
public class UniformAssignor implements PartitionAssignor {
    private static final Logger LOG = LoggerFactory.getLogger(UniformAssignor.class);
    public static final String UNIFORM_ASSIGNOR_NAME = "uniform";

    @Override
    public String name() {
        return UNIFORM_ASSIGNOR_NAME;
    }

    /**
     * Perform the group assignment given the current members and
     * topics metadata.
     *
     * @param assignmentSpec                The assignment specification that included member metadata.
     * @param subscribedTopicDescriber      The topic and cluster metadata describer {@link SubscribedTopicDescriber}.
     * @return The new target assignment for the group.
     */
    @Override
    public GroupAssignment assign(
        AssignmentSpec assignmentSpec,
        SubscribedTopicDescriber subscribedTopicDescriber
    ) throws PartitionAssignorException {
        AbstractAssignmentBuilder assignmentBuilder;

        if (assignmentSpec.members().isEmpty())
            return new GroupAssignment(Collections.emptyMap());

        if (allSubscriptionsEqual(assignmentSpec.members())) {
            LOG.debug("Detected that all members are subscribed to the same set of topics, invoking the "
                + "optimized assignment algorithm");
            assignmentBuilder = new OptimizedUniformAssignmentBuilder(assignmentSpec, subscribedTopicDescriber);
        } else {
            assignmentBuilder = new GeneralUniformAssignmentBuilder(assignmentSpec, subscribedTopicDescriber);
            LOG.debug("Detected that all members are subscribed to a different set of topics, invoking the "
                + "general assignment algorithm");
        }

        return assignmentBuilder.buildAssignment();
    }

    /**
     * Determines if all members are subscribed to the same list of topic Ids.
     *
     * @param members       Members mapped to their respective {@code AssignmentMemberSpec}.
     * @return true if all members have the same subscription list of topic Ids,
     *         false otherwise.
     */
    private boolean allSubscriptionsEqual(Map<String, AssignmentMemberSpec> members) {
        Set<Uuid> firstSubscriptionSet = new HashSet<>(members.values().iterator().next().subscribedTopicIds());
        for (AssignmentMemberSpec memberSpec : members.values()) {
            if (!firstSubscriptionSet.containsAll(memberSpec.subscribedTopicIds())) {
                return false;
            }
        }
        return true;
    }

    /**
     * The assignment builder is used to construct the target assignment.
     *
     * This class contains common utility methods and a class for obtaining and storing rack information.
     */
     protected static abstract class AbstractAssignmentBuilder {
        protected abstract GroupAssignment buildAssignment();

        /**
         * Determines if rack-aware assignment is appropriate based on the provided rack information.
         *
         * @param memberRacks           Racks where members are located.
         * @param partitionRacks        Racks where partitions are located.
         * @param racksPerPartition     Map of partitions to their associated racks.
         *
         * @return {@code true} if rack-aware assignment should be applied; {@code false} otherwise.
         */
        protected static boolean useRackAwareAssignment(
            Set<String> memberRacks,
            Set<String> partitionRacks,
            Map<TopicIdPartition, Set<String>> racksPerPartition
        ) {
            if (memberRacks.isEmpty() || Collections.disjoint(memberRacks, partitionRacks))
                return false;
            else {
                return !racksPerPartition.values().stream().allMatch(partitionRacks::equals);
            }
        }

        /**
         * Adds the topic's partition to the member's target assignment.
         */
        protected static void addPartitionToAssignment(
            int partition,
            Uuid topicId,
            String memberId,
            Map<String, MemberAssignment> targetAssignment
        ) {
            targetAssignment.get(memberId)
                .targetPartitions()
                .computeIfAbsent(topicId, __ -> new HashSet<>())
                .add(partition);
        }

        /**
         * Constructs a list of {@code TopicIdPartition} for each topic Id based on its partition count.
         *
         * @param allTopicIds                   The subscribed topic Ids.
         * @param subscribedTopicDescriber      Utility to fetch the partition count for a given topic.
         *
         * @return List of sorted {@code TopicIdPartition} for all provided topic Ids.
         */
        protected static List<TopicIdPartition> allTopicIdPartitions(
            Collection<Uuid> allTopicIds,
            SubscribedTopicDescriber subscribedTopicDescriber
        ) {
            List<TopicIdPartition> allTopicIdPartitions = new ArrayList<>();
            // Sorted so that partitions from each topic can be distributed amongst its subscribers equally.
            allTopicIds.stream().sorted().forEach(topic ->
                IntStream.range(0, subscribedTopicDescriber.numPartitions(topic))
                    .forEach(i -> allTopicIdPartitions.add(new TopicIdPartition(topic, i))
                )
            );
            
            return allTopicIdPartitions;
        }

        /**
         * Represents the rack information of members and partitions along with utility methods
         * to facilitate rack-aware assignment strategies for a given consumer group.
         */
        protected static class RackInfo {
            /**
             * Map of every member to its rack.
             */
            protected final Map<String, String> memberRacks;
            /**
             * Map of every partition to a list of its racks.
             */
            protected final Map<TopicIdPartition, Set<String>> partitionRacks;
            /**
             * Number of members with the same rack as the partition.
             */
            private final Map<TopicIdPartition, List<String>> membersWithSameRackAsPartition;
            /**
             * Indicates if a rack aware assignment can be done.
             * True if racks are defined for both members and partitions and there is an intersection between the sets.
             */
            protected final boolean useRackStrategy;

            /**
             * Constructs rack information based on the assignment specification and subscribed topics.
             *
             * @param assignmentSpec                The current assignment specification.
             * @param subscribedTopicDescriber      Topic and partition metadata of the subscribed topics.
             * @param topicIds                      List of topic Ids.
             */
            public RackInfo(
                AssignmentSpec assignmentSpec,
                SubscribedTopicDescriber subscribedTopicDescriber,
                Set<Uuid> topicIds
            ) {
                Map<String, List<String>> membersByRack = new HashMap<>();
                assignmentSpec.members().forEach((memberId, assignmentMemberSpec) ->
                    assignmentMemberSpec.rackId().filter(r -> !r.isEmpty()).ifPresent(
                        rackId -> membersByRack.computeIfAbsent(rackId, __ -> new ArrayList<>()).add(memberId)
                    )
                );

                Set<String> allPartitionRacks;
                Map<TopicIdPartition, Set<String>> partitionRacks;
                List<TopicIdPartition> topicIdPartitions = allTopicIdPartitions(topicIds, subscribedTopicDescriber);

                if (membersByRack.isEmpty()) {
                    allPartitionRacks = Collections.emptySet();
                    partitionRacks = Collections.emptyMap();
                } else {
                    partitionRacks = new HashMap<>();
                    allPartitionRacks = new HashSet<>();
                    topicIdPartitions.forEach(tp -> {
                        Set<String> racks = subscribedTopicDescriber.racksForPartition(tp.topicId(), tp.partition());
                        partitionRacks.put(tp, racks);
                        if (!racks.isEmpty()) allPartitionRacks.addAll(racks);
                    });
                }

                if (useRackAwareAssignment(membersByRack.keySet(), allPartitionRacks, partitionRacks)) {
                    this.memberRacks = new HashMap<>(assignmentSpec.members().size());
                    membersByRack.forEach((rack, rackMembers) -> rackMembers.forEach(c -> memberRacks.put(c, rack)));
                    this.partitionRacks = partitionRacks;
                    useRackStrategy = true;
                } else {
                    this.memberRacks = Collections.emptyMap();
                    this.partitionRacks = Collections.emptyMap();
                    useRackStrategy = false;
                }

                this.membersWithSameRackAsPartition = partitionRacks.entrySet().stream()
                    .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().stream()
                            .flatMap(rack -> membersByRack.getOrDefault(rack, Collections.emptyList()).stream())
                            .collect(Collectors.toList())
                    ));
            }

            /**
             * @return List of members with the same rack as any of the provided partition's replicas.
             */
            protected List<String> getMembersWithMatchingRack(TopicIdPartition topicIdPartition) {
                return membersWithSameRackAsPartition.getOrDefault(topicIdPartition, Collections.emptyList());
            }

            /**
             * Determines if there's a mismatch between the member's rack and the partition's replica racks.
             *
             * <p> Racks are considered mismatched under the following conditions: (returns {@code true}):
             * <ul>
             *     <li> Member lacks an associated rack. </li>
             *     <li> Partition lacks associated replica racks. </li>
             *     <li> Member's rack isn't among the partition's replica racks. </li>
             * </ul>
             *
             * @param memberId      The member Id.
             * @param tp            The topic partition.
             * @return {@code true} for a mismatch; {@code false} if member and partition racks exist and align.
             */
            protected boolean racksMismatch(String memberId, TopicIdPartition tp) {
                String memberRack = memberRacks.get(memberId);
                Set<String> replicaRacks = partitionRacks.get(tp);
                return memberRack == null || (replicaRacks == null || !replicaRacks.contains(memberRack));
            }

            /**
             * Sort partitions in ascending order by number of members with matching racks.
             *
             * @param partitions    The list of partitions to be sorted.
             * @return A sorted list of partitions with potential members in the same rack.
             */
            protected List<TopicIdPartition> sortPartitionsByRackMembers(List<TopicIdPartition> partitions) {
                if (membersWithSameRackAsPartition.isEmpty())
                    return partitions;

                return partitions.stream()
                    .filter(tp -> {
                        Integer count = membersWithSameRackAsPartition.get(tp).size();
                        return count != null && count > 0;
                    })
                    .sorted(Comparator.comparing(tp -> membersWithSameRackAsPartition
                        .getOrDefault(tp, Collections.emptyList()).size()
                    ))
                    .collect(Collectors.toList());
            }

            @Override
            public String toString() {
                return "RackInfo(" +
                    "memberRacks=" + memberRacks +
                    ", partitionRacks=" + partitionRacks +
                    ")";
            }
        }

    }
}

