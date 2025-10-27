package org.redisson.connection;

import org.junit.jupiter.api.Test;
import org.redisson.api.NodeType;
import org.redisson.client.RedisClient;
import org.redisson.client.RedisClientConfig;
import org.redisson.config.AwsReplicatedServersConfig;
import org.redisson.config.Config;
import org.redisson.misc.RedisURI;
import software.amazon.awssdk.services.elasticache.ElastiCacheClient;
import software.amazon.awssdk.services.elasticache.model.DescribeReplicationGroupsRequest;
import software.amazon.awssdk.services.elasticache.model.DescribeReplicationGroupsResponse;
import software.amazon.awssdk.services.elasticache.model.Endpoint;
import software.amazon.awssdk.services.elasticache.model.NodeGroup;
import software.amazon.awssdk.services.elasticache.model.NodeGroupMember;
import software.amazon.awssdk.services.elasticache.model.ReplicationGroup;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AwsReplicatedConnectionManagerTest {

    @Test
    public void testApplyTopologyAddsAndRemovesReplicas() {
        Config rootConfig = new Config();
        AwsReplicatedServersConfig awsConfig = rootConfig.useAwsReplicatedServers()
                .setReplicationGroupId("rg-1")
                .setRegion("us-east-1")
                .setElastiCacheClient(mock(ElastiCacheClient.class));

        MasterSlaveEntry entryMock = mock(MasterSlaveEntry.class);
        TestAwsManager manager = new TestAwsManager(awsConfig, rootConfig, entryMock);

        RedisURI masterUri = new RedisURI("redis://master.example.com:6379");
        RedisURI existingReplica = new RedisURI("redis://replica-existing.example.com:6379");
        RedisURI newReplica = new RedisURI("redis://replica-new.example.com:6379");
        RedisURI obsoleteReplica = new RedisURI("redis://old.example.com:6379");

        Set<RedisURI> currentSlaves = new HashSet<>();
        currentSlaves.add(existingReplica);
        currentSlaves.add(obsoleteReplica);

        when(entryMock.hasSlave(any(RedisURI.class))).thenAnswer(invocation -> {
            RedisURI uri = invocation.getArgument(0);
            return currentSlaves.contains(uri);
        });

        doAnswer(invocation -> {
            RedisURI uri = invocation.getArgument(0);
            manager.replicaAdditions.add(uri);
            currentSlaves.add(uri);
            return CompletableFuture.completedFuture(null);
        }).when(entryMock).addSlave(any(RedisURI.class), anyString());

        doAnswer(invocation -> {
            RedisURI uri = invocation.getArgument(0);
            manager.slaveUpInvocations.add(uri);
            return CompletableFuture.completedFuture(currentSlaves.contains(uri));
        }).when(entryMock).slaveUpAsync(any(RedisURI.class));

        doAnswer(invocation -> {
            RedisURI uri = invocation.getArgument(0);
            manager.slaveDownInvocations.add(uri);
            currentSlaves.remove(uri);
            return true;
        }).when(entryMock).slaveDown(any(RedisURI.class));

        ClientConnectionsEntry existingEntry = mock(ClientConnectionsEntry.class);
        RedisClient existingClient = mock(RedisClient.class);
        RedisClientConfig existingClientConfig = mock(RedisClientConfig.class);
        when(existingClient.getConfig()).thenReturn(existingClientConfig);
        when(existingClientConfig.getAddress()).thenReturn(existingReplica);
        when(existingEntry.getNodeType()).thenReturn(NodeType.SLAVE);
        when(existingEntry.getClient()).thenReturn(existingClient);

        ClientConnectionsEntry obsoleteEntry = mock(ClientConnectionsEntry.class);
        RedisClient obsoleteClient = mock(RedisClient.class);
        RedisClientConfig obsoleteConfig = mock(RedisClientConfig.class);
        when(obsoleteClient.getConfig()).thenReturn(obsoleteConfig);
        when(obsoleteConfig.getAddress()).thenReturn(obsoleteReplica);
        when(obsoleteEntry.getNodeType()).thenReturn(NodeType.SLAVE);
        when(obsoleteEntry.getClient()).thenReturn(obsoleteClient);

        Collection<ClientConnectionsEntry> entries = Arrays.asList(existingEntry, obsoleteEntry);
        when(entryMock.getAllEntries()).thenReturn(entries);

        AwsReplicatedConnectionManager.AwsTopology topology =
                new AwsReplicatedConnectionManager.AwsTopology(
                        masterUri,
                        Arrays.asList(existingReplica, newReplica)
                );

        manager.applyTopology(topology).join();

        assertThat(manager.masterChanges).containsExactly(masterUri);
        assertThat(manager.masterIpChecks).containsExactly(masterUri);

        assertThat(manager.replicaAdditions).containsExactly(newReplica);
        assertThat(manager.slaveUpInvocations).containsExactly(existingReplica);
        assertThat(manager.replicaIpChecks).containsExactlyInAnyOrder(existingReplica, newReplica);

        assertThat(manager.slaveDownInvocations).containsExactly(obsoleteReplica);

        assertThat(manager.config.getMasterAddress()).isEqualTo(masterUri.toString());
        assertThat(manager.config.getSlaveAddresses()).containsExactlyInAnyOrder(
                existingReplica.toString(),
                newReplica.toString()
        );
    }

    @Test
    public void testResolveReplicationGroupWithDiscoveryEndpoint() {
        Config rootConfig = new Config();
        ElastiCacheClient client = mock(ElastiCacheClient.class);

        AwsReplicatedServersConfig awsConfig = rootConfig.useAwsReplicatedServers()
                .setRegion("us-east-1")
                .setDiscoveryEndpoint("redis://target.example.com:6379")
                .setElastiCacheClient(client);

        Endpoint otherEndpoint = Endpoint.builder()
                .address("other.example.com")
                .port(6379)
                .build();
        ReplicationGroup otherGroup = ReplicationGroup.builder()
                .replicationGroupId("rg-other")
                .nodeGroups(NodeGroup.builder()
                        .nodeGroupId("0001")
                        .primaryEndpoint(otherEndpoint)
                        .nodeGroupMembers(NodeGroupMember.builder()
                                .cacheClusterId("cluster-other")
                                .currentRole("primary")
                                .build())
                        .build())
                .build();

        Endpoint targetEndpoint = Endpoint.builder()
                .address("target.example.com")
                .port(6379)
                .build();
        ReplicationGroup targetGroup = ReplicationGroup.builder()
                .replicationGroupId("rg-target")
                .nodeGroups(NodeGroup.builder()
                        .nodeGroupId("0002")
                        .primaryEndpoint(targetEndpoint)
                        .nodeGroupMembers(NodeGroupMember.builder()
                                .cacheClusterId("cluster-target")
                                .currentRole("primary")
                                .build())
                        .build())
                .build();

        DescribeReplicationGroupsResponse response = DescribeReplicationGroupsResponse.builder()
                .replicationGroups(otherGroup, targetGroup)
                .build();

        when(client.describeReplicationGroups(any(DescribeReplicationGroupsRequest.class))).thenReturn(response);

        MasterSlaveEntry entryMock = mock(MasterSlaveEntry.class);
        TestAwsManager manager = new TestAwsManager(awsConfig, rootConfig, entryMock);

        ReplicationGroup resolved = manager.resolve();
        assertThat(resolved.replicationGroupId()).isEqualTo("rg-target");
    }

    @Test
    public void testRefreshTopologySkipsAwsWhenRedisStable() {
        Config rootConfig = new Config();
        AwsReplicatedServersConfig awsConfig = rootConfig.useAwsReplicatedServers()
                .setReplicationGroupId("rg-stable")
                .setRegion("us-east-1")
                .setElastiCacheClient(mock(ElastiCacheClient.class));

        RefreshTestAwsManager manager = new RefreshTestAwsManager(awsConfig, rootConfig);
        try {
            manager.enqueuePollResult(false);
            manager.setForceAws(false);

            manager.refreshTopology().join();

            assertThat(manager.describeCallCount()).isZero();
            assertThat(manager.applyCallCount()).isZero();
        } finally {
            manager.shutdown(0, 0, TimeUnit.MILLISECONDS);
        }
    }

    @Test
    public void testRefreshTopologyInvokesAwsWhenRedisSignalsChange() {
        Config rootConfig = new Config();
        AwsReplicatedServersConfig awsConfig = rootConfig.useAwsReplicatedServers()
                .setReplicationGroupId("rg-change")
                .setRegion("us-east-1")
                .setElastiCacheClient(mock(ElastiCacheClient.class));

        RefreshTestAwsManager manager = new RefreshTestAwsManager(awsConfig, rootConfig);
        try {
            manager.enqueuePollResult(true);
            manager.setForceAws(false);

            manager.refreshTopology().join();

            assertThat(manager.describeCallCount()).isEqualTo(1);
            assertThat(manager.applyCallCount()).isEqualTo(1);
        } finally {
            manager.shutdown(0, 0, TimeUnit.MILLISECONDS);
        }
    }

    @Test
    public void testRefreshTopologyInvokesAwsWhenRefreshIsForced() {
        Config rootConfig = new Config();
        AwsReplicatedServersConfig awsConfig = rootConfig.useAwsReplicatedServers()
                .setReplicationGroupId("rg-forced")
                .setRegion("us-east-1")
                .setElastiCacheClient(mock(ElastiCacheClient.class));

        RefreshTestAwsManager manager = new RefreshTestAwsManager(awsConfig, rootConfig);
        try {
            manager.enqueuePollResult(false);
            manager.setForceAws(true);

            manager.refreshTopology().join();

            assertThat(manager.describeCallCount()).isEqualTo(1);
            assertThat(manager.applyCallCount()).isEqualTo(1);
        } finally {
            manager.shutdown(0, 0, TimeUnit.MILLISECONDS);
        }
    }

    private static class TestAwsManager extends AwsReplicatedConnectionManager {

        private final MasterSlaveEntry entry;
        private final RedisClient masterClientMock = mock(RedisClient.class);

        final List<RedisURI> masterChanges = new ArrayList<>();
        final List<RedisURI> masterIpChecks = new ArrayList<>();
        final List<RedisURI> replicaIpChecks = new ArrayList<>();
        final List<RedisURI> replicaAdditions = new ArrayList<>();
        final List<RedisURI> slaveUpInvocations = new ArrayList<>();
        final List<RedisURI> slaveDownInvocations = new ArrayList<>();

        TestAwsManager(AwsReplicatedServersConfig cfg, Config configCopy, MasterSlaveEntry entry) {
            super(cfg, configCopy);
            this.entry = entry;
        }

        @Override
        protected CompletableFuture<RedisClient> changeMaster(int slot, RedisURI address) {
            masterChanges.add(address);
            return CompletableFuture.completedFuture(masterClientMock);
        }

        @Override
        public MasterSlaveEntry getEntry(int slot) {
            return entry;
        }

        @Override
        protected CompletableFuture<Void> checkMasterIp(RedisURI masterUri) {
            masterIpChecks.add(masterUri);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        protected CompletableFuture<Void> checkReplicaIp(RedisURI replicaUri) {
            replicaIpChecks.add(replicaUri);
            return CompletableFuture.completedFuture(null);
        }

        ReplicationGroup resolve() {
            return resolveReplicationGroup();
        }
    }

    private static class RefreshTestAwsManager extends AwsReplicatedConnectionManager {

        private final Queue<Boolean> pollResults = new ArrayDeque<>();
        private final AtomicBoolean forceAws = new AtomicBoolean();
        private final AtomicInteger describeCalls = new AtomicInteger();
        private final AtomicInteger applyCalls = new AtomicInteger();

        RefreshTestAwsManager(AwsReplicatedServersConfig cfg, Config configCopy) {
            super(cfg, configCopy);
        }

        void enqueuePollResult(boolean value) {
            pollResults.add(value);
        }

        void setForceAws(boolean value) {
            forceAws.set(value);
        }

        int describeCallCount() {
            return describeCalls.get();
        }

        int applyCallCount() {
            return applyCalls.get();
        }

        @Override
        protected CompletableFuture<Boolean> pollRedisTopology() {
            Boolean next = pollResults.poll();
            return CompletableFuture.completedFuture(next != null && next);
        }

        @Override
        protected boolean isAwsRefreshDue() {
            return forceAws.get();
        }

        @Override
        protected AwsTopology loadAwsTopology() {
            describeCalls.incrementAndGet();
            return new AwsTopology(new RedisURI("redis://forced-master.example.com:6379"), Collections.emptyList());
        }

        @Override
        protected CompletableFuture<Void> applyTopology(AwsTopology topology) {
            applyCalls.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }
    }
}
