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
import org.apache.kafka.coordinator.group.consumer.Assignment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class GroupSpecImplTest {
    private Map<String, MemberSubscriptionSpecImpl> members;
    private SubscriptionType subscriptionType;
    private Map<Uuid, Map<Integer, String>> invertedTargetAssignment;
    private GroupSpecImpl groupSpec;
    private Uuid topicId;
    private final String testMember = "test-member";

    @BeforeEach
    void setUp() {
        members = new HashMap<>();
        subscriptionType = SubscriptionType.HOMOGENEOUS;
        invertedTargetAssignment = new HashMap<>();
        topicId = Uuid.randomUuid();

        members.put(testMember,  new MemberSubscriptionSpecImpl(
            Optional.empty(),
            mkSet(topicId),
            Assignment.EMPTY
        ));

        groupSpec = new GroupSpecImpl(
            members,
            subscriptionType,
            invertedTargetAssignment
        );
    }

    @Test
    void testMemberIds() {
        assertEquals(members.keySet(), groupSpec.memberIds());
    }

    @Test
    void testSubscriptionType() {
        assertEquals(subscriptionType, groupSpec.subscriptionType());
    }

    @Test
    void testIsPartitionAssigned() {
        Map<Integer, String> partitionMap = new HashMap<>();
        partitionMap.put(1, "test-member");
        invertedTargetAssignment.put(topicId, partitionMap);

        assertTrue(groupSpec.isPartitionAssigned(topicId, 1));
        assertFalse(groupSpec.isPartitionAssigned(topicId, 2));
        assertFalse(groupSpec.isPartitionAssigned(Uuid.randomUuid(), 2));
    }

    @Test
    void testMemberSubscriptionSpec() {
        assertEquals(members.get(testMember), groupSpec.memberSubscription(testMember));
        assertThrows(IllegalStateException.class, () -> groupSpec.memberSubscription("unknown-member"));
    }

    @Test
    void testCurrentMemberAssignment() {
        Map<Uuid, Set<Integer>> topicPartitions = new HashMap<>();
        topicPartitions.put(
            topicId,
            mkSet(0, 1)
        );
        members.put(testMember, new MemberSubscriptionSpecImpl(
            Optional.empty(),
            mkSet(topicId),
            new Assignment(topicPartitions)
        ));

        assertEquals(topicPartitions, groupSpec.memberAssignment(testMember));
        assertEquals(Collections.emptyMap(), groupSpec.memberAssignment("unknown-member"));
    }
}
