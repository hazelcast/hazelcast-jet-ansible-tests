/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.tests.client.subset.routing;


import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClusterRoutingConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.connection.tcp.RoutingMode;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.tests.common.AbstractClientSoakTest;
import com.hazelcast.jet.tests.common.ClusterType;
import com.hazelcast.jet.tests.common.ConditionVerifierWithTimeout;
import com.hazelcast.jet.tests.common.ConditionVerifierWithTimeout.ConditionResult;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.hazelcast.client.config.RoutingStrategy.PARTITION_GROUPS;
import static com.hazelcast.jet.tests.common.ClusterType.DYNAMIC;
import static com.hazelcast.jet.tests.common.ClusterType.STABLE;
import static com.hazelcast.jet.tests.common.Util.sleepMinutes;

public class SubsetRoutingTest extends AbstractClientSoakTest {
    private static final int DEFAULT_LOG_VERIFICATION_COUNT_THRESHOLD = 5;
    private static final int DEFAULT_SLEEP_BETWEEN_VALIDATIONS_IN_MINUTES = 1;
    private static final int DEFAULT_MAX_WAIT_FOR_EXPECTED_CONNECTION_IN_MINUTES = 10;
    private static final int SLEEP_BETWEEN_CONNECTION_ASSERTION_ATTEMPTS_IN_SECONDS = 10;
    private static final ThreadLocal<UUID> CONNECTED_MEMBER_UUID = ThreadLocal.withInitial(() -> null);

    private int logVerificationCountThreshold;
    private int sleepBeforeValidationInMinutes;
    private long maxWaitForExpectedConnectionInMinutes;
    private int sleepBetweenConnectionAssertionAttemptsInSeconds;

    public static void main(String[] args) throws Exception {
        new SubsetRoutingTest().run(args);
    }



    @Override
    public ClientConfig localClientConfig() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig()
                .setClusterRoutingConfig(new ClusterRoutingConfig()
                        .setRoutingMode(RoutingMode.MULTI_MEMBER)
                        .setRoutingStrategy(PARTITION_GROUPS));
        return clientConfig;
    }

    @Override
    protected void init() throws Exception {
        logVerificationCountThreshold = propertyInt("logVerificationCountThreshold",
                DEFAULT_LOG_VERIFICATION_COUNT_THRESHOLD);
        sleepBeforeValidationInMinutes = propertyInt("sleepBeforeValidationInMinutes",
                DEFAULT_SLEEP_BETWEEN_VALIDATIONS_IN_MINUTES);
        maxWaitForExpectedConnectionInMinutes = propertyInt("maxWaitForExpectedConnectionInMinutes",
                DEFAULT_MAX_WAIT_FOR_EXPECTED_CONNECTION_IN_MINUTES);
        sleepBetweenConnectionAssertionAttemptsInSeconds = propertyInt("sleepBetweenConnectionAssertionAttemptsInSeconds",
                SLEEP_BETWEEN_CONNECTION_ASSERTION_ATTEMPTS_IN_SECONDS);
    }

    @Override
    protected void beforeTest(HazelcastInstance client, ClusterType clusterType) throws Exception {
    }

    @Override
    protected void test(HazelcastInstance client, ClusterType clusterType, String name) throws Throwable {
        long begin = System.currentTimeMillis();
        String logPrefix = "[" + name + "] ";

        HazelcastClientInstanceImpl clientInstance = getHazelcastClientInstanceImpl(client);

        long validationCount = 0;

        while (System.currentTimeMillis() - begin < durationInMillis) {
            assertEffectiveMembersAndActiveConnections(clientInstance);
            assertRequestToCluster(clientInstance);

            validationCount++;
            if (validationCount % logVerificationCountThreshold == 0) {
                logger.info(logPrefix + "Validations count: " + validationCount);
            }
            sleepMinutes(sleepBeforeValidationInMinutes);

        }
        logger.info(logPrefix + "Final validations count: " + validationCount);
    }

    private void assertRequestToCluster(HazelcastClientInstanceImpl clientInstance) {
        AtomicReference<Throwable> lastException = new AtomicReference<>();

        ConditionVerifierWithTimeout<Void> verifier = new ConditionVerifierWithTimeout<>(
                Duration.ofMinutes(maxWaitForExpectedConnectionInMinutes),
                Duration.ofSeconds(sleepBetweenConnectionAssertionAttemptsInSeconds));

        verifier
                //This method throws OperationTimeoutException for unavailable cluster
                .condition(() -> new ConditionResult<>(
                        clientInstance.getMap(this.getClass().getSimpleName()).isEmpty()
                ))
                .onException(lastException::set)
                .onTimeout(() -> {
                    if (lastException.get() != null) {
                        throw new IllegalStateException("Timed out waiting for cluster active connection",
                                lastException.get());
                    }
                    throw new IllegalStateException("Timed out waiting for cluster active connection");
                })
                .execute();

    }

    @Override
    protected List<ClusterType> runOnClusters() {
        return List.of(DYNAMIC, STABLE);
    }

    private void assertEffectiveMembersAndActiveConnections(HazelcastClientInstanceImpl clientInstance) {
        Duration maxLoopDuration = Duration.ofMinutes(maxWaitForExpectedConnectionInMinutes);
        Duration sleepBetweenAttempts = Duration.ofSeconds(sleepBetweenConnectionAssertionAttemptsInSeconds);

        ConditionVerifierWithTimeout<ConnectionsContext> verifier = new ConditionVerifierWithTimeout<>(maxLoopDuration,
                sleepBetweenAttempts);

        verifier.condition(() -> {
                    Collection<Member> effectiveMemberList = getEffectiveMemberList(clientInstance);
                    int activeConnections = getActiveConnections(clientInstance);
                    return new ConditionResult<>(
                            effectiveMemberList.size() == 1 && activeConnections == 1,
                            new ConnectionsContext(activeConnections, effectiveMemberList));
                })
                .onConditionPass(context -> {
                    UUID newConnectedMemberUuid = context.effectiveMemberList
                            .iterator().next().getUuid();
                    if (!newConnectedMemberUuid.equals(CONNECTED_MEMBER_UUID.get())) {
                        logger.info("Client is connected to a new member: "
                                + newConnectedMemberUuid + " previously: " + CONNECTED_MEMBER_UUID.get());
                    }
                    CONNECTED_MEMBER_UUID.set(newConnectedMemberUuid);
                })
                .onConditionNotPassedYet(
                        context -> logger.info("Waiting for expected connection... Currently,"
                                + " active connections: "
                                + context.activeConnections + " effective members: "
                                + membersToString(context.effectiveMemberList)))
                .onTimeout(
                        () -> {
                            throw new IllegalStateException("Timeout during waiting for expected connection " +
                                    "and effective members." +
                                    " Active connections: " + getActiveConnections(clientInstance) +
                                    " Effective members:" + membersToString(getEffectiveMemberList(clientInstance)) +
                                    " Previously client was connected to " + CONNECTED_MEMBER_UUID.get() + " member");
                        })
                .execute();
    }

    private static String membersToString(Collection<Member> effectiveMemberList) {
        return '[' + effectiveMemberList
                .stream()
                .map(Member::getUuid)
                .map(Objects::toString)
                .collect(Collectors.joining(",")) + ']';
    }

    private static int getActiveConnections(HazelcastClientInstanceImpl clientInstance) {
        return clientInstance.getConnectionManager().getActiveConnections().size();
    }

    private static Collection<Member> getEffectiveMemberList(HazelcastClientInstanceImpl clientInstance) {
        return clientInstance.getClientClusterService().getEffectiveMemberList();
    }

    @Override
    protected void teardown(Throwable t) throws Exception {
    }

    public static HazelcastClientInstanceImpl getHazelcastClientInstanceImpl(HazelcastInstance hz) {
        HazelcastClientInstanceImpl impl = null;
        if (hz instanceof HazelcastClientProxy) {
            impl = ((HazelcastClientProxy) hz).client;
        } else if (hz instanceof HazelcastClientInstanceImpl) {
            impl = (HazelcastClientInstanceImpl) hz;
        }
        return impl;
    }

    public static class ConnectionsContext {
        private final int activeConnections;
        private final Collection<Member> effectiveMemberList;

        public ConnectionsContext(int activeConnections, Collection<Member> effectiveMemberList) {
            this.activeConnections = activeConnections;
            this.effectiveMemberList = effectiveMemberList;
        }
    }

}
