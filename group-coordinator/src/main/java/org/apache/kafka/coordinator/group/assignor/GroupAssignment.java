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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The partition assignment for a consumer group.
 */
public class GroupAssignment {
    /**
     * The member assignments keyed by member id.
     */
    private final Map<String, MemberAssignment> members;
    private final Map<String, List<TopicIdPartition>> newClientTypeAssignment;

    public GroupAssignment(
        Map<String, MemberAssignment> members
    ) {
        Objects.requireNonNull(members);
        this.members = members;
        this.newClientTypeAssignment = Collections.emptyMap();
    }

    public GroupAssignment(
        Map<String, MemberAssignment> members,
        Map<String, List<TopicIdPartition>> newClientTypeAssignment
    ) {
        Objects.requireNonNull(members);
        this.members = members;
        Objects.requireNonNull(newClientTypeAssignment);
        this.newClientTypeAssignment = newClientTypeAssignment;
    }

    /**
     * @return Member assignments keyed by member Ids.
     */
    public Map<String, MemberAssignment> members() {
        return members;
    }

    public Map<String, List<TopicIdPartition>> getNewClientTypeAssignment() {return newClientTypeAssignment;}
    public Map<String, MemberAssignment> convertNewClientTypeAssignment(
        Map<String, List<TopicIdPartition>> newClientTypeAssignment
    ) {
        Map<String, MemberAssignment> convertedMembers = new HashMap<>();
        for (Map.Entry<String, List<TopicIdPartition>> entry : newClientTypeAssignment.entrySet()) {
            String memberId = entry.getKey();
            List<TopicIdPartition> partitions = entry.getValue();

            // Convert List<TopicIdPartition> to Map<Uuid, Set<Integer>>
            Map<Uuid, Set<Integer>> topicPartitionsMap = new HashMap<>();
            for (TopicIdPartition topicIdPartition : partitions) {
                topicPartitionsMap.computeIfAbsent(topicIdPartition.topicId(), k -> new HashSet<>())
                    .add(topicIdPartition.partitionId());
            }

            convertedMembers.put(memberId, new MemberAssignment(topicPartitionsMap));
        }
        return convertedMembers;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GroupAssignment)) return false;
        GroupAssignment that = (GroupAssignment) o;
        return members.equals(that.members) && getNewClientTypeAssignment().equals(that.getNewClientTypeAssignment());
    }

    @Override
    public int hashCode() {
        return Objects.hash(members, getNewClientTypeAssignment());
    }

    @Override
    public String toString() {
        return "GroupAssignment(members=" + members +
            ", newClientTypeAssignment=" + newClientTypeAssignment +
            ')';
    }
}
