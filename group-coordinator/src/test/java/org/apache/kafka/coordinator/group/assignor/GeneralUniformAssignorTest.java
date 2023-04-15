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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class GeneralUniformAssignorTest {

    private final UniformAssignor assignor = new UniformAssignor();

    private final String topic1Name = "topic1";
    private final Uuid topic1Uuid = Uuid.fromString("T1-A4s3VTwiI5CTbEp6POw");

    private final String topic2Name = "topic2";
    private final Uuid topic2Uuid = Uuid.fromString("T2-B4s3VTwiI5YHbPp6YUe");

    private final String topic3Name = "topic3";
    private final Uuid topic3Uuid = Uuid.fromString("T3-CU8fVTLCz5YMkLoDQsa");

    private final String consumerA = "A";
    private final String consumerB = "B";
    private final String consumerC = "C";

    @Test
    public void testOneConsumerNoTopicSubscription() {
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(topic1Name, 3));
        Map<String, AssignmentMemberSpec> members = new HashMap<>();
        List<Uuid> subscribedTopics = new ArrayList<>();
        members.computeIfAbsent(consumerA, k -> new AssignmentMemberSpec(Optional.empty(), Optional.empty(), subscribedTopics, new HashMap<>()));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment groupAssignment = assignor.assign(assignmentSpec);

        assertTrue(groupAssignment.members().isEmpty());
    }
}
