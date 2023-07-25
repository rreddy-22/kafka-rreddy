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

import org.apache.kafka.coordinator.group.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class UniformAssignor implements PartitionAssignor {

    private static final Logger log = LoggerFactory.getLogger(UniformAssignor.class);
    public static final String UNIFORM_ASSIGNOR_NAME = "uniform";
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
        AbstractAssignmentBuilder assignmentBuilder;
        if (allSubscriptionsEqual(assignmentSpec.members())) {
            log.debug("Detected that all consumers were subscribed to same set of topics, invoking the "
                + "optimized assignment algorithm");
            assignmentBuilder = new OptimizedAssignmentBuilder(assignmentSpec, subscribedTopicDescriber);
        } else {
            assignmentBuilder = new GeneralAssignmentBuilder(assignmentSpec, subscribedTopicDescriber);
        }
        return assignmentBuilder.build();
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

    protected static abstract class AbstractAssignmentBuilder {

        final Map<String, AssignmentMemberSpec> metadataPerMember;
        final SubscribedTopicDescriber subscribedTopicDescriber;

        AbstractAssignmentBuilder(AssignmentSpec assignmentSpec, SubscribedTopicDescriber subscribedTopicDescriber) {
            this.metadataPerMember = assignmentSpec.members();
            this.subscribedTopicDescriber = subscribedTopicDescriber;
        }

        /**
         * Builds the assignment.
         *
         * @return Map from each member to the list of partitions assigned to them.
         */
        abstract GroupAssignment build();

        protected List<TopicIdPartition> getAllTopicPartitions(List<Uuid> listAllTopics) {
            List<TopicIdPartition> allPartitions = new ArrayList<>();
            for (Uuid topic : listAllTopics) {
                int partitionCount = subscribedTopicDescriber.numPartitions(topic);
                for (int i = 0; i < partitionCount; ++i) {
                    allPartitions.add(new TopicIdPartition(topic, i));
                }
            }
            return allPartitions;
        }
    }
}
