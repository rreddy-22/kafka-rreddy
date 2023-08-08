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

import org.apache.kafka.common.TopicPartition;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class UniformAssignor implements PartitionAssignor {

    private static final Logger log = LoggerFactory.getLogger(UniformAssignor.class);
    public static final String UNIFORM_ASSIGNOR_NAME = "uniform";

    // Used only in unit tests to verify rack-aware assignment when all racks have all partitions.
    boolean preferRackAwareLogic;
    @Override
    public String name() {
        return UNIFORM_ASSIGNOR_NAME;
    }

    /**
     * Perform the group assignment given the current members and
     * topic metadata.
     *
     * @param assignmentSpec           The member assignment spec.
     * @param subscribedTopicDescriber The topic and cluster metadata describer {@link SubscribedTopicDescriber}.
     * @return The new assignment for the group.
     */
    @Override
    public GroupAssignment assign(AssignmentSpec assignmentSpec, SubscribedTopicDescriber subscribedTopicDescriber) throws PartitionAssignorException {
        if (allSubscriptionsEqual(assignmentSpec.members())) {
            log.debug("Detected that all consumers were subscribed to same set of topics, invoking the "
                + "optimized assignment algorithm");
            OptimizedUniformAssignor optimizedUniformAssignor = new OptimizedUniformAssignor(assignmentSpec, subscribedTopicDescriber);
            return optimizedUniformAssignor.build();
        } else {
            GeneralUniformAssignor generalUniformAssignor = new GeneralUniformAssignor();
            return generalUniformAssignor.build();
        }
    }

    private boolean allSubscriptionsEqual(Map<String, AssignmentMemberSpec> members) {
        boolean areAllSubscriptionsEqual = true;
        Collection<Uuid> firstSubscriptionList = members.values().iterator().next().subscribedTopicIds();
        for (AssignmentMemberSpec memberSpec : members.values()) {
            if (!firstSubscriptionList.equals(memberSpec.subscribedTopicIds())) {
                areAllSubscriptionsEqual = false;
                break;
            }
        }
        return areAllSubscriptionsEqual;
    }
    protected static <K, V> void put(Map<K, List<V>> map, K key, V value) {
        List<V> list = map.computeIfAbsent(key, k -> new ArrayList<>());
        list.add(value);
    }

    protected boolean useRackAwareAssignment(Set<String> consumerRacks, Set<String> partitionRacks, Map<TopicIdPartition, Set<String>> racksPerPartition) {
        if (consumerRacks.isEmpty() || Collections.disjoint(consumerRacks, partitionRacks))
            return false;
        else if (preferRackAwareLogic)
            return true;
        else {
            return !racksPerPartition.values().stream().allMatch(partitionRacks::equals);
        }
    }
    protected List<TopicIdPartition> getAllTopicPartitions(List<Uuid> listAllTopics, SubscribedTopicDescriber subscribedTopicDescriber) {
        List<TopicIdPartition> allPartitions = new ArrayList<>();
        for (Uuid topic : listAllTopics) {
            int partitionCount = subscribedTopicDescriber.numPartitions(topic);
            for (int i = 0; i < partitionCount; ++i) {
                allPartitions.add(new TopicIdPartition(topic, i));
            }
        }
        return allPartitions;
    }

    protected class RackInfo {
        protected final Map<String, String> consumerRacks;
        protected final Map<TopicIdPartition, Set<String>> partitionRacks;
        private final Map<TopicIdPartition, Integer> numConsumersByPartition;

        public RackInfo(AssignmentSpec assignmentSpec, SubscribedTopicDescriber subscribedTopicDescriber, List<Uuid> topicIds) {
            Map<String, List<String>> consumersByRack = new HashMap<>();
            assignmentSpec.members().forEach((memberId, assignmentMemberSpec) ->
                assignmentMemberSpec.rackId().filter(r -> !r.isEmpty()).ifPresent(rackId -> put(consumersByRack, rackId, memberId)));

            Set<String> allPartitionRacks;
            List<TopicIdPartition> topicIdPartitions = getAllTopicPartitions(topicIds, subscribedTopicDescriber);
            Map<TopicIdPartition, Set<String>> partitionRacks;
            if (consumersByRack.isEmpty()) {
                allPartitionRacks = Collections.emptySet();
                partitionRacks = Collections.emptyMap();
            } else {
                partitionRacks = new HashMap<>();
                allPartitionRacks = new HashSet<>();
                topicIdPartitions.forEach(tp -> {
                    Set<String> racks = subscribedTopicDescriber.racksForPartition(tp.topicId(), tp.partition());
                    partitionRacks.put(tp, racks);
                    allPartitionRacks.addAll(racks);
                });
            }

            if (useRackAwareAssignment(consumersByRack.keySet(), allPartitionRacks, partitionRacks)) {
                this.consumerRacks = new HashMap<>(assignmentSpec.members().size());
                consumersByRack.forEach((rack, rackConsumers) -> rackConsumers.forEach(c -> consumerRacks.put(c, rack)));
                this.partitionRacks = partitionRacks;
            } else {
                this.consumerRacks = Collections.emptyMap();
                this.partitionRacks = Collections.emptyMap();
            }
            numConsumersByPartition = partitionRacks.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().stream()
                    .map(r -> consumersByRack.getOrDefault(r, Collections.emptyList()).size())
                    .reduce(0, Integer::sum)));
        }

        protected boolean racksMismatch(String consumer, TopicIdPartition tp) {
            String consumerRack = consumerRacks.get(consumer);
            Set<String> replicaRacks = partitionRacks.get(tp);
            return consumerRack != null && (replicaRacks == null || !replicaRacks.contains(consumerRack));
        }

        protected List<TopicIdPartition> sortPartitionsByRackConsumers(List<TopicIdPartition> partitions) {
            if (numConsumersByPartition.isEmpty())
                return partitions;
            // Return a sorted linked list of partitions to enable fast updates during rack-aware assignment
            List<TopicIdPartition> sortedPartitions = new LinkedList<>(partitions);
            sortedPartitions.sort(Comparator.comparing(tp -> numConsumersByPartition.getOrDefault(tp, 0)));
            return sortedPartitions;
        }

        private int nextRackConsumer(TopicIdPartition tp, List<String> consumerList, int firstIndex) {
            Set<String> racks = partitionRacks.get(tp);
            if (racks == null || racks.isEmpty())
                return -1;
            for (int i = 0; i < consumerList.size(); i++) {
                int index = (firstIndex + i) % consumerList.size();
                String consumer = consumerList.get(index);
                String consumerRack = consumerRacks.get(consumer);
                if (consumerRack != null && racks.contains(consumerRack))
                    return index;
            }
            return -1;
        }
    }
}

