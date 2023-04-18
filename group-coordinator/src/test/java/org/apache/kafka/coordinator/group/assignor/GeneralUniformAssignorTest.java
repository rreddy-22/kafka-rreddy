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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GeneralUniformAssignorTest {

    private final UniformAssignor assignor = new UniformAssignor();

    private final Uuid topic1Uuid = Uuid.fromString("T1-A4s3VTwiI5CTbEp6POw");

    private final Uuid topic2Uuid = Uuid.fromString("T2-B4s3VTwiI5YHbPp6YUe");

    private final Uuid topic3Uuid = Uuid.fromString("T3-CU8fVTLCz5YMkLoDQsa");

    private final String consumerA = "A";
    private final String consumerB = "B";
    private final String consumerC = "C";

    @Test
    public void testOneConsumerNoTopicSubscription() {
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(3));
        Map<String, AssignmentMemberSpec> members = new HashMap<>();
        List<Uuid> subscribedTopics = new ArrayList<>();
        members.computeIfAbsent(consumerA, k -> new AssignmentMemberSpec(Optional.empty(), Optional.empty(), subscribedTopics, new HashMap<>()));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment groupAssignment = assignor.assign(assignmentSpec);

        assertTrue(groupAssignment.members().isEmpty());
    }
    @Test
    public void testOneConsumerNonexistentTopic() {
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(3));
        Map<String, AssignmentMemberSpec> members = new HashMap<>();
        List<Uuid> subscribedTopics = new ArrayList<>();
        subscribedTopics.add(topic2Uuid);
        members.computeIfAbsent(consumerA, k -> new AssignmentMemberSpec(Optional.empty(), Optional.empty(), subscribedTopics, new HashMap<>()));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment groupAssignment = assignor.assign(assignmentSpec);

        assertTrue(groupAssignment.members().isEmpty());
    }

    @Test
    public void testFirstAssignmentThreeConsumersThreeTopics() {
        // A -> T1, T2 // B -> T3 // C -> T2, T3 // T1 -> 3 Partitions // T2 -> 3 Partitions // T3 -> 2 Partitions
        // Topics
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(3));
        topics.put(topic2Uuid, new AssignmentTopicMetadata(3));
        topics.put(topic3Uuid, new AssignmentTopicMetadata(2));
        // Members
        Map<String, AssignmentMemberSpec> members = new HashMap<>();
        // Consumer A
        List<Uuid> subscribedTopicsA = new ArrayList<>(Arrays.asList(topic1Uuid, topic2Uuid));
        members.put(consumerA, new AssignmentMemberSpec(
                         Optional.empty(),
                            Optional.empty(),
                                   subscribedTopicsA,
                                   new HashMap<>()));

        // Consumer B
        List<Uuid> subscribedTopicsB = new ArrayList<>(Collections.singletonList(topic3Uuid));
        members.put(consumerB, new AssignmentMemberSpec(
                             Optional.empty(),
                                Optional.empty(),
                                       subscribedTopicsB,
                                       new HashMap<>()));

        // Consumer C
        List<Uuid> subscribedTopicsC = new ArrayList<>(Arrays.asList(topic2Uuid, topic3Uuid));
        members.put(consumerC, new AssignmentMemberSpec(
                            Optional.empty(),
                               Optional.empty(),
                                      subscribedTopicsC,
                                      new HashMap<>()));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment computedAssignment = assignor.assign(assignmentSpec);

        // Assert computed assignments with expected values
        // A -> T1 -> [0, 1, 2]
        // B -> T3 -> [0, 1]
        // C -> T2 -> [0, 1, 2]

        // Topic 1
        assertEquals(new HashSet<>(Arrays.asList(0, 1, 2)),
                        computedAssignment.members().get(consumerA).targetPartitions().get(topic1Uuid)
        );
        // Topic 2
        assertEquals(new HashSet<>(Arrays.asList(0, 1)),
                computedAssignment.members().get(consumerB).targetPartitions().get(topic3Uuid)
        );
        // Topic 3
        assertEquals(new HashSet<>(Arrays.asList(0, 1, 2)),
                computedAssignment.members().get(consumerC).targetPartitions().get(topic2Uuid)
        );
    }
    @Test
    public void testFirstAssignmentTwoConsumersTwoTopics() {
        // A -> T1, T2 // B -> T3 // C -> T2, T3 // T1 -> 3 Partitions // T2 -> 3 Partitions // T3 -> 2 Partitions
        // Topics
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(6));
        topics.put(topic2Uuid, new AssignmentTopicMetadata(3));

        // Members
        Map<String, AssignmentMemberSpec> members = new HashMap<>();
        // Consumer A
        List<Uuid> subscribedTopicsA = new ArrayList<>(Arrays.asList(topic1Uuid, topic2Uuid));
        members.put(consumerA, new AssignmentMemberSpec(
                Optional.empty(),
                Optional.empty(),
                subscribedTopicsA,
                new HashMap<>()));

        // Consumer B
        List<Uuid> subscribedTopicsB = new ArrayList<>(Collections.singletonList(topic1Uuid));
        members.put(consumerB, new AssignmentMemberSpec(
                Optional.empty(),
                Optional.empty(),
                subscribedTopicsB,
                new HashMap<>()));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment computedAssignment = assignor.assign(assignmentSpec);

        // Assert computed assignments with expected values
        // A -> T1 -> [0, 1, 2]
        // B -> T3 -> [0, 1]
        // C -> T2 -> [0, 1, 2]

        // Topic 1
        assertEquals(new HashSet<>(Collections.singletonList(1)),
                computedAssignment.members().get(consumerA).targetPartitions().get(topic1Uuid)
        );
        assertEquals(new HashSet<>(Arrays.asList(0, 2, 3, 4, 5)),
                computedAssignment.members().get(consumerB).targetPartitions().get(topic1Uuid)
        );

        // Topic 2
        assertEquals(new HashSet<>(Arrays.asList(0, 1, 2)),
                computedAssignment.members().get(consumerA).targetPartitions().get(topic2Uuid)
        );
    }

    @Test
    public void testReassignmentWhenOneConsumerAddedAfterInitialAssignmentWithTwoConsumersTwoTopics() {
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(6));
        topics.put(topic2Uuid, new AssignmentTopicMetadata(3));

        Map<String, AssignmentMemberSpec> members = new HashMap<>();
        // Consumer A
        List<Uuid> subscribedTopicsA = new ArrayList<>(Arrays.asList(topic1Uuid, topic2Uuid));
        Map<Uuid, Set<Integer>> currentAssignmentForA = new HashMap<>();
        currentAssignmentForA.put(topic1Uuid, new HashSet<>(Collections.singletonList(1)));
        currentAssignmentForA.put(topic2Uuid, new HashSet<>(Arrays.asList(0, 1, 2)));
        members.computeIfAbsent(consumerA, k -> new AssignmentMemberSpec(Optional.empty(), Optional.empty(), subscribedTopicsA, currentAssignmentForA));
        // Consumer B
        List<Uuid> subscribedTopicsB = new ArrayList<>(Collections.singletonList(topic1Uuid));
        Map<Uuid, Set<Integer>> currentAssignmentForB = new HashMap<>();
        currentAssignmentForB.put(topic1Uuid, new HashSet<>(Arrays.asList(0, 2, 3, 4, 5)));
        members.computeIfAbsent(consumerB, k -> new AssignmentMemberSpec(Optional.empty(), Optional.empty(), subscribedTopicsB, currentAssignmentForB));

        // Add a new consumer to trigger a re-assignment
        // Consumer C
        List<Uuid> subscribedTopicsC = new ArrayList<>(Arrays.asList(topic1Uuid, topic2Uuid));
        members.put(consumerC, new AssignmentMemberSpec(Optional.empty(), Optional.empty(), subscribedTopicsC, new HashMap<>()));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment computedAssignment = assignor.assign(assignmentSpec);

        Map<Uuid, Set<Set<Integer>>> expectedAssignment = new HashMap<>();
        // Topic 1 Partitions Assignment
        expectedAssignment.computeIfAbsent(topic1Uuid, k -> new HashSet<>()).add(new HashSet<>(Collections.singletonList(0)));
        expectedAssignment.computeIfAbsent(topic1Uuid, k -> new HashSet<>()).add(new HashSet<>(Collections.singletonList(2)));
        expectedAssignment.computeIfAbsent(topic1Uuid, k -> new HashSet<>()).add(new HashSet<>(Collections.singletonList(1)));
        // Topic 2 Partitions Assignment
        expectedAssignment.computeIfAbsent(topic2Uuid, k -> new HashSet<>()).add(new HashSet<>(Collections.singletonList(0)));
        expectedAssignment.computeIfAbsent(topic2Uuid, k -> new HashSet<>()).add(new HashSet<>(Collections.singletonList(2)));
        expectedAssignment.computeIfAbsent(topic2Uuid, k -> new HashSet<>()).add(new HashSet<>(Collections.singletonList(1)));

        // Test for stickiness
        //assertEquals(computedAssignment.members().get(consumerB).targetPartitions(), new HashMap<>(currentAssignmentForB), "Stickiness test failed for Consumer B");
        //assertTrue(computedAssignment.members().get(consumerA).targetPartitions().get(topic1Uuid).contains(0), "Stickiness test failed for Consumer A");
        // Co-partition join property test ensures that A retained the same partition for all topics.
    }

}
