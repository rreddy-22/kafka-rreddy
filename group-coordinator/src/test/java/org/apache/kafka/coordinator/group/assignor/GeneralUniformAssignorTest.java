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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
        Map<Uuid, AssignmentTopicMetadata> topics = Collections.singletonMap(topic1Uuid, new AssignmentTopicMetadata(3));
        Map<String, AssignmentMemberSpec> members = Collections.singletonMap(
            consumerA,
            new AssignmentMemberSpec(
                Optional.empty(),
                Optional.empty(),
                Collections.emptyList(),
                Collections.emptyMap()
            )
        );

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment groupAssignment = assignor.assign(assignmentSpec);

        assertEquals(Collections.emptyMap(), groupAssignment.members());
    }

    @Test
    public void testFirstAssignmentThreeConsumersThreeTopics() {
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(3));
        topics.put(topic2Uuid, new AssignmentTopicMetadata(3));
        topics.put(topic3Uuid, new AssignmentTopicMetadata(2));

        Map<String, AssignmentMemberSpec> members = new TreeMap<>();

        members.put(consumerA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid),
            Collections.emptyMap()
        ));

        members.put(consumerB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Collections.singletonList(topic3Uuid),
            Collections.emptyMap()
        ));

        members.put(consumerC, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic2Uuid, topic3Uuid),
            Collections.emptyMap()
        ));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment computedAssignment = assignor.assign(assignmentSpec);

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();

        expectedAssignment.put(consumerA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 1, 2)
        ));

        expectedAssignment.put(consumerB, mkAssignment(
            mkTopicAssignment(topic3Uuid, 0, 1)
        ));

        expectedAssignment.put(consumerC, mkAssignment(
            mkTopicAssignment(topic2Uuid, 0, 1, 2)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
    }
    @Test
    public void testFirstAssignmentTwoConsumersTwoTopics() {
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(6));
        topics.put(topic2Uuid, new AssignmentTopicMetadata(3));

        Map<String, AssignmentMemberSpec> members = new HashMap<>();

        members.put(consumerA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid),
            Collections.emptyMap()
        ));

        members.put(consumerB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid),
            Collections.emptyMap()
        ));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment computedAssignment = assignor.assign(assignmentSpec);

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();

        expectedAssignment.put(consumerA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 1),
            mkTopicAssignment(topic2Uuid, 0, 1, 2)
        ));

        expectedAssignment.put(consumerB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 2, 3, 4, 5)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testReassignmentWhenOneConsumerAddedAfterInitialAssignmentWithTwoConsumersTwoTopics() {
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(6));
        topics.put(topic2Uuid, new AssignmentTopicMetadata(3));

        Map<String, AssignmentMemberSpec> members = new HashMap<>();
        Map<Uuid, Set<Integer>> currentAssignmentForA = mkAssignment(
            mkTopicAssignment(topic1Uuid, 1),
            mkTopicAssignment(topic2Uuid, 0, 1, 2)
        );

        members.put(consumerA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid),
            currentAssignmentForA
        ));

        Map<Uuid, Set<Integer>> currentAssignmentForB = mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 2, 3, 4, 5)
        );

        members.put(consumerB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid),
            currentAssignmentForB
        ));

        // Add a new consumer to trigger a re-assignment
        members.put(consumerC, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid),
            Collections.emptyMap()
        ));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment computedAssignment = assignor.assign(assignmentSpec);

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();

        expectedAssignment.put(consumerA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 1),
            mkTopicAssignment(topic2Uuid, 0, 1)
        ));

        expectedAssignment.put(consumerB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 2, 3)
        ));

        expectedAssignment.put(consumerC, mkAssignment(
            mkTopicAssignment(topic1Uuid, 4, 5),
            mkTopicAssignment(topic2Uuid, 2)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    private void assertAssignment(Map<String, Map<Uuid, Set<Integer>>> expectedAssignment, GroupAssignment computedGroupAssignment) {
        assertEquals(expectedAssignment.size(), computedGroupAssignment.members().size());
        for (String memberId : computedGroupAssignment.members().keySet()) {
            Map<Uuid, Set<Integer>> computedAssignmentForMember = computedGroupAssignment.members().get(memberId).targetPartitions();
            assertEquals(expectedAssignment.get(memberId), computedAssignmentForMember);
        }
    }
}
