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
import org.apache.kafka.coordinator.group.api.assignor.ConsumerGroupPartitionAssignor;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.assignor.MemberSubscription;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;
import org.apache.kafka.coordinator.group.consumer.MemberAssignmentImpl;

import java.util.ArrayList;
import java.util.Collections;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A range assignor assigns contiguous partition ranges to members of a consumer group such that :
 * 1. Each subscribed member receives at least one partition from that topic.
 * 2. Each member receives the same partition number from every subscribed topic when co-partitioning is possible.
 *
 * Co-partitioning is possible when the below conditions are satisfied:
 * 1. ALl the members are subscribed to the same set of topics.
 * 2. All the topics have the same number of partitions.
 *
 * Co-partitioning is useful in performing joins on data streams.
 *
 * <p>For example, suppose there are two members M0 and M1, two topics T1 and T2, and each topic has 3 partitions.
 *
 * <p>The co-partitioned assignment will be:
 * <ul>
 * <li<code>    M0: [T1P0, T1P1, T2P0, T2P1]    </code></li>
 * <li><code>   M1: [T1P2, T2P2]                </code></li>
 * </ul>
 *
 * Since the introduction of static membership, we could leverage <code>member.instance.id</code> to make the
 * assignment behavior more sticky.
 * For the above example, after one rolling bounce, the group coordinator will attempt to assign new member Ids towards
 * members, for example if <code>M0</code> -&gt; <code>M3</code> <code>M1</code> -&gt; <code>M2</code>.
 *
 * <p>The assignment could be completely shuffled to:
 * <ul>
 * <li><code>   M3 (was M0): [T1P2, T2P2]               (before it was [T1P0, T1P1, T2P0, T2P1])  </code>
 * <li><code>   M2 (was M1): [T1P0, T1P1, T2P0, T2P1]   (before it was [T1P2, T2P2])  </code>
 * </ul>
 *
 * The assignment change was caused by the change of <code>member.id</code> relative order, and
 * can be avoided by setting the instance.id.
 * Members will have individual instance Ids <code>I0</code>, <code>I1</code>. As long as
 * 1. Number of members remain the same.
 * 2. Topic metadata doesn't change.
 * 3. Subscription pattern doesn't change for any member.
 *
 * <p>The assignment will always be:
 * <ul>
 * <li><code>   I0: [T1P0, T1P1, T2P0, T2P1]    </code>
 * <li><code>   I1: [T1P2, T2P2]                </code>
 * </ul>
 * <p>
 */
public class RangeAssignor implements ConsumerGroupPartitionAssignor {
    public static final String RANGE_ASSIGNOR_NAME = "range";

    @Override
    public String name() {
        return RANGE_ASSIGNOR_NAME;
    }

    private static class TopicMetadata {
        public Uuid topicId;
        public int numPartitions;
        public int numMembers;

        public int quota = -1;
        public int extra = -1;
        public int nextRange = 0;

        void maybeComputeQuota() {
            if (quota != -1) return;

            quota = numPartitions / numMembers;
            extra = numPartitions % numMembers;
        }

        @Override
        public String toString() {
            return "TopicMetadata{" +
                "topicId=" + topicId +
                ", numPartitions=" + numPartitions +
                ", numMembers=" + numMembers +
                ", quota=" + quota +
                ", extra=" + extra +
                ", nextRange=" + nextRange +
                '}';
        }
    }

    /**
     * Assigns partitions to members of a homogeneous group. All members are subscribed to the same set of topics.
     * Assignment will be co-partitioned when all the topics have an equal number of partitions.
     */
    private GroupAssignment assignHomogeneousGroup(
        GroupSpec groupSpec,
        SubscribedTopicDescriber subscribedTopicDescriber
    ) throws PartitionAssignorException {
        List<String> memberIds = sortMemberIds(groupSpec);

        MemberSubscription subs = groupSpec.memberSubscription(memberIds.get(0));
        Set<Uuid> subscribedTopics = new HashSet<>(subs.subscribedTopicIds());
        List<TopicMetadata> topics = new ArrayList<>(subscribedTopics.size());
        int numMembers = groupSpec.memberIds().size();

        for (Uuid topicId : subscribedTopics) {
            TopicMetadata m = new TopicMetadata();
            m.topicId = topicId;
            m.numPartitions = subscribedTopicDescriber.numPartitions(topicId);
            if (m.numPartitions == -1) {
                throw new PartitionAssignorException("Member is subscribed to a non-existent topic");
            }
            m.numMembers = numMembers;
            topics.add(m);
        }

        Map<String, MemberAssignment> assignments = new HashMap<>((int) ((groupSpec.memberIds().size() / 0.75f) + 1));

        for (String memberId : memberIds) {
            Map<Uuid, Set<Integer>> assignment = new HashMap<>((int) ((subscribedTopics.size() / 0.75f) + 1));
            for (TopicMetadata metadata : topics) {
                metadata.maybeComputeQuota();

                if (metadata.nextRange >= metadata.numPartitions) {
                    assignment.put(metadata.topicId, Collections.emptySet());
                } else {
                    int start = metadata.nextRange;
                    int end = Math.min(start + metadata.quota, metadata.numPartitions);
                    if (metadata.extra > 0) {
                        end++;
                        metadata.extra--;
                    }
                    metadata.nextRange = end;
                    assignment.put(metadata.topicId, new RangeSet(start, end));
                }
            }
            assignments.put(memberId, new MemberAssignmentImpl(assignment));
        }

        return new GroupAssignment(assignments);
    }

    /**
     * Assigns partitions to members of a heterogeneous group. Not all members are subscribed to the same topics.
     */
    private GroupAssignment assignHeterogeneousGroup(
        GroupSpec groupSpec,
        SubscribedTopicDescriber subscribedTopicDescriber
    ) throws PartitionAssignorException {

        List<String> memberIds = sortMemberIds(groupSpec);

        Map<Uuid, TopicMetadata> topics = new HashMap<>();

        for (String memberId : memberIds) {
            MemberSubscription subs = groupSpec.memberSubscription(memberId);
            for (Uuid topicId : subs.subscribedTopicIds()) {
                TopicMetadata metadata = topics.computeIfAbsent(topicId, __ -> {
                    TopicMetadata m = new TopicMetadata();
                    m.topicId = topicId;
                    m.numPartitions = subscribedTopicDescriber.numPartitions(topicId);
                    if (m.numPartitions == -1) {
                        throw new PartitionAssignorException("Member is subscribed to a non-existent topic");
                    }
                    return m;
                });
                metadata.numMembers++;
            }
        }

        Map<String, MemberAssignment> assignments = new HashMap<>((int) ((groupSpec.memberIds().size() / 0.75f) + 1));

        for (String memberId : memberIds) {
            MemberSubscription subs = groupSpec.memberSubscription(memberId);
            Map<Uuid, Set<Integer>> assignment = new HashMap<>(subs.subscribedTopicIds().size());
            for (Uuid topicId : subs.subscribedTopicIds()) {
                TopicMetadata metadata = topics.get(topicId);
                metadata.maybeComputeQuota();

                if (metadata.nextRange >= metadata.numPartitions) {
                    assignment.put(metadata.topicId, Collections.emptySet());
                } else {
                    int start = metadata.nextRange;
                    int end = Math.min(start + metadata.quota, metadata.numPartitions);
                    if (metadata.extra > 0) {
                        end++;
                        metadata.extra--;
                    }
                    metadata.nextRange = end;
                    assignment.put(metadata.topicId, new RangeSet(start, end));
                }
            }
            assignments.put(memberId, new MemberAssignmentImpl(assignment));
        }

        return new GroupAssignment(assignments);
    }

    /**
     * Sorts the member Ids in the group based on their instance Id if present, otherwise by member Id.
     * This is done to ensure that the relative ordering of members doesn't change with static members
     * thus resulting in a sticky assignment.
     *
     * @param groupSpec     The group specification containing the member information.
     * @return a sorted list of member Ids.
     */
    private List<String> sortMemberIds(
        GroupSpec groupSpec
    ) {
        List<String> sortedMemberIds = new ArrayList<>(groupSpec.memberIds());
        sortedMemberIds.sort((memberId1, memberId2) -> {
            Optional<String> instanceId1 = groupSpec.memberSubscription(memberId1).instanceId();
            Optional<String> instanceId2 = groupSpec.memberSubscription(memberId2).instanceId();

            if (instanceId1.isPresent() && instanceId2.isPresent()) {
                return instanceId1.get().compareTo(instanceId2.get());
            } else if (instanceId1.isPresent()) {
                return -1;
            } else if (instanceId2.isPresent()) {
                return 1;
            } else {
                return memberId1.compareTo(memberId2);
            }
        });
        return sortedMemberIds;
    }

    /**
     * Assigns partitions to members based on their topic subscriptions and the properties of a range assignor:
     *
     * @param groupSpec                     The group specification containing the member information.
     * @param subscribedTopicDescriber      The describer for subscribed topics to get the number of partitions.
     * @return The group's assignment with the partition assignments for each member.
     * @throws PartitionAssignorException if any member is subscribed to a non-existent topic.
     */
    @Override
    public GroupAssignment assign(
        GroupSpec groupSpec,
        SubscribedTopicDescriber subscribedTopicDescriber
    ) throws PartitionAssignorException {
        if (groupSpec.subscriptionType() == SubscriptionType.HOMOGENEOUS) {
            return assignHomogeneousGroup(groupSpec, subscribedTopicDescriber);
        } else {
            return assignHeterogeneousGroup(groupSpec, subscribedTopicDescriber);
        }
    }
}