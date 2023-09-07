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
import java.util.Set;
import java.util.stream.Collectors;

public class GeneralUniformAssignmentBuilder extends UniformAssignor.AbstractAssignmentBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(GeneralUniformAssignmentBuilder.class);
    /**
     * The assignment specification which includes member metadata.
     */
    private final AssignmentSpec assignmentSpec;
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
     * Tracks the current owner of each partition.
     * Current refers to the existing assignment.
     */
    private final Map<TopicIdPartition, String> currentPartitionOwners;
    /**
     * The new assignment that will be returned.
     */
    private final Map<String, MemberAssignment> newAssignment;

    public GeneralUniformAssignmentBuilder(AssignmentSpec assignmentSpec, SubscribedTopicDescriber subscribedTopicDescriber) {
        this.assignmentSpec = assignmentSpec;
        this.subscribedTopicDescriber = subscribedTopicDescriber;
        this.subscriptionIds = assignmentSpec.members().values().stream()
            .flatMap(memberSpec -> memberSpec.subscribedTopicIds().stream())
            .collect(Collectors.toSet());
        this.rackInfo = new RackInfo(assignmentSpec, subscribedTopicDescriber, subscriptionIds);
        this.unassignedPartitions = new ArrayList<>();
        this.currentPartitionOwners = new HashMap<>();
        this.newAssignment = new HashMap<>();
    }

    @Override
    protected GroupAssignment buildAssignment() {
        if (subscriptionIds.isEmpty()) {
            LOG.info("The subscription list is empty, returning an empty assignment");
            return new GroupAssignment(Collections.emptyMap());
        }

        assignmentSpec.members().keySet().forEach(memberId ->
            newAssignment.put(memberId, new MemberAssignment(new HashMap<>())
        ));
        // When rack awareness is enabled, this only contains sticky partitions with matching rack.
        // Otherwise, it contains all sticky partitions.
        Set<TopicIdPartition> allAssignedStickyPartitions = computeAssignedStickyPartitions();
        return new GroupAssignment(newAssignment);
    }

    /**
     * Topic Ids are sorted in descending order based on the value:
     * totalPartitions/number of subscribed members.
     * If the above value is the same then topic Ids are sorted in ascending order of number of subscribers.
     */
    List<Uuid> sortedTopics() {
        Map<Uuid, List<String>> membersPerTopic = new HashMap<>();
        assignmentSpec.members().forEach((memberId, memberMetadata) -> {
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

        return membersPerTopic.keySet().stream()
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
    private Set<TopicIdPartition> computeAssignedStickyPartitions() {
        Set<TopicIdPartition> allAssignedStickyPartitions = new HashSet<>();

        assignmentSpec.members().forEach((memberId, assignmentMemberSpec) ->
            assignmentMemberSpec.assignedPartitions().forEach((topicId, currentAssignment) -> {
                if (subscriptionIds.contains(topicId)) {
                    currentAssignment.forEach(partition -> {
                        TopicIdPartition topicIdPartition = new TopicIdPartition(topicId, partition);
                        if (rackInfo.useRackStrategy && rackInfo.racksMismatch(memberId, topicIdPartition)) {
                            currentPartitionOwners.put(topicIdPartition, memberId);
                        } else {
                            newAssignment.get(memberId)
                                .targetPartitions()
                                .computeIfAbsent(topicId, __ -> new HashSet<>())
                                .add(partition);
                            allAssignedStickyPartitions.add(topicIdPartition);
                        }
                    });
                } else {
                    LOG.debug("The topic " + topicId + " is no longer present in the subscribed topics list");
                }
            })
        );
        return allAssignedStickyPartitions;
    }
}
