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
import org.apache.kafka.coordinator.group.consumer.SubscribedTopicMetadata;
import org.apache.kafka.coordinator.group.consumer.TopicMetadata;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import static org.apache.kafka.coordinator.group.RecordHelpersTest.mkMapOfPartitionRacks;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class GeneralUniformAssignmentBuilderTest {
    private final UniformAssignor assignor = new UniformAssignor();
    private final Uuid topic1Uuid = Uuid.fromString("T1-A4s3VTwiI5CTbEp6POw");
    private final Uuid topic2Uuid = Uuid.fromString("T2-B4s3VTwiI5YHbPp6YUe");
    private final Uuid topic3Uuid = Uuid.fromString("T3-CU8fVTLCz5YMkLoDQsa");
    private final String topic1Name = "topic1";
    private final String topic2Name = "topic2";
    private final String topic3Name = "topic3";
    private final String memberA = "A";
    private final String memberB = "B";
    private final String memberC = "C";

    @Test
    public void testSortedTopicsThreeMembersThreeTopicsWithMemberAndPartitionRacks() {
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put(topic1Uuid, new TopicMetadata(
            topic1Uuid,
            topic1Name,
            6,
            mkMapOfPartitionRacks(6)
        ));
        topicMetadata.put(topic2Uuid, new TopicMetadata(
            topic2Uuid,
            topic2Name,
            3,
            mkMapOfPartitionRacks(3)
        ));
        topicMetadata.put(topic3Uuid, new TopicMetadata(
            topic3Uuid,
            topic3Name,
            2,
            mkMapOfPartitionRacks(2)
        ));

        Map<String, AssignmentMemberSpec> members = new TreeMap<>();
        members.put(memberA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.of("rack1"),
            Collections.singletonList(topic1Uuid),
            Collections.emptyMap()
        ));
        members.put(memberB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.of("rack2"),
            Arrays.asList(topic1Uuid, topic2Uuid),
            Collections.emptyMap()
        ));
        members.put(memberC, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.of("rack3"),
            Arrays.asList(topic1Uuid, topic3Uuid),
            Collections.emptyMap()
        ));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);
        GeneralUniformAssignmentBuilder generalAssignmentBuilder =
            new GeneralUniformAssignmentBuilder(assignmentSpec, subscribedTopicMetadata);

        // T1 = 6/3 || T2 = 3/1 || T3 = 2/1
        List<Uuid> sortedTopics = generalAssignmentBuilder.sortedTopics();
        assertEquals(Arrays.asList(topic2Uuid, topic3Uuid, topic1Uuid), sortedTopics);
    }
}
