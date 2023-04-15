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

import org.apache.kafka.coordinator.group.common.RackAwareTopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class UniformAssignor implements PartitionAssignor {

    private static final Logger log = LoggerFactory.getLogger(UniformAssignor.class);
    public static final String UNIFORM_ASSIGNOR_NAME = "uniform";
    @Override
    public String name() {
        return UNIFORM_ASSIGNOR_NAME;
    }

    @Override
    public GroupAssignment assign(AssignmentSpec assignmentSpec) throws PartitionAssignorException {
        AbstractAssignmentBuilder assignmentBuilder;
        if (allSubscriptionsEqual(assignmentSpec.members)) {
            log.debug("Detected that all consumers were subscribed to same set of topics, invoking the "
                    + "optimized assignment algorithm");
            assignmentBuilder = new OptimizedAssignmentBuilder(assignmentSpec);
        } else {
            assignmentBuilder = new GeneralAssignmentBuilder(assignmentSpec);
        }
        return assignmentBuilder.build();
    }

    private boolean allSubscriptionsEqual(Map<String, AssignmentMemberSpec> members) {
        boolean areAllSubscriptionsEqual = true;
        List<Uuid> firstSubscriptionList = members.values().iterator().next().subscribedTopics;
        for (AssignmentMemberSpec memberSpec : members.values()) {
            if (!firstSubscriptionList.equals(memberSpec.subscribedTopics)) {
                areAllSubscriptionsEqual = false;
                break;
            }
        }
        return areAllSubscriptionsEqual;
    }

    protected static abstract class AbstractAssignmentBuilder {

        final Map<Uuid, AssignmentTopicMetadata> metadataPerTopic;
        final Map<String, AssignmentMemberSpec> metadataPerMember;

        AbstractAssignmentBuilder(AssignmentSpec assignmentSpec) {
            this.metadataPerTopic = assignmentSpec.topics;
            this.metadataPerMember = assignmentSpec.members;
        }

        /**
         * Builds the assignment.
         *
         * @return Map from each member to the list of partitions assigned to them.
         */
        abstract GroupAssignment build();

        protected List<RackAwareTopicIdPartition> getAllTopicPartitions(List<Uuid> listAllTopics) {
            List<RackAwareTopicIdPartition> allPartitions = new ArrayList<>();
            for (Uuid topic : listAllTopics) {
                int partitionCount = metadataPerTopic.get(topic).numPartitions;
                for (int i = 0; i < partitionCount; ++i) {
                    allPartitions.add(new RackAwareTopicIdPartition(topic, i, null));
                }
            }
            return allPartitions;
        }
    }
}
