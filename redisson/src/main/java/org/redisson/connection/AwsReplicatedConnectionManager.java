/**
 * Copyright (c) 2013-2024 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson.connection;

import io.netty.util.Timeout;
import org.redisson.api.NodeType;
import org.redisson.client.RedisClient;
import org.redisson.client.RedisConnection;
import org.redisson.client.RedisConnectionException;
import org.redisson.client.codec.StringCodec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.config.AwsReplicatedServersConfig;
import org.redisson.config.BaseMasterSlaveServersConfig;
import org.redisson.config.Config;
import org.redisson.config.MasterSlaveServersConfig;
import org.redisson.misc.RedisURI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.elasticache.ElastiCacheClient;
import software.amazon.awssdk.services.elasticache.ElastiCacheClientBuilder;
import software.amazon.awssdk.services.elasticache.model.CacheCluster;
import software.amazon.awssdk.services.elasticache.model.CacheNode;
import software.amazon.awssdk.services.elasticache.model.DescribeCacheClustersRequest;
import software.amazon.awssdk.services.elasticache.model.DescribeCacheClustersResponse;
import software.amazon.awssdk.services.elasticache.model.DescribeReplicationGroupsRequest;
import software.amazon.awssdk.services.elasticache.model.DescribeReplicationGroupsResponse;
import software.amazon.awssdk.services.elasticache.model.Endpoint;
import software.amazon.awssdk.services.elasticache.model.NodeGroup;
import software.amazon.awssdk.services.elasticache.model.NodeGroupMember;
import software.amazon.awssdk.services.elasticache.model.ReplicationGroup;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * ConnectionManager implementation with AWS ElastiCache runtime discovery.
 *
 * It keeps Redisson topology in sync with AWS replication group membership and reacts to failovers,
 * replicas being added or removed, and IP changes.
 *
 * @author Johno Crawford (johno@sulake.com)
 */
public class AwsReplicatedConnectionManager extends MasterSlaveConnectionManager {

    private static final Logger log = LoggerFactory.getLogger(AwsReplicatedConnectionManager.class);

    private static final long REDIS_MONITOR_INTERVAL_MS = 2_000L;
    private static final long MIN_FORCED_AWS_REFRESH_INTERVAL_MS = 60_000L;
    private static final long MAX_SHARED_REFRESH_WAIT_MS = 20_000L;

    private static final ConcurrentHashMap<AwsTopologyKey, AwsTopologyView> SHARED_TOPOLOGY_CACHE = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<AwsTopologyKey, CompletableFuture<AwsTopologyView>> SHARED_REFRESHES = new ConcurrentHashMap<>();

    private final AwsReplicatedServersConfig cfg;

    private final ElastiCacheClient elastiCacheClient;

    private final boolean managedClient;

    private final AtomicReference<RedisURI> currentMaster = new AtomicReference<>();
    private final AtomicReference<Set<String>> currentReplicas = new AtomicReference<>(Collections.emptySet());
    private final AtomicReference<RedisReplicationState> redisInfoSnapshot = new AtomicReference<>();

    private volatile Timeout monitorFuture;

    private final long redisMonitorIntervalMs;
    private final long forcedAwsRefreshIntervalNanos;
    private volatile long lastAwsRefreshTimeNanos;

    AwsReplicatedConnectionManager(AwsReplicatedServersConfig cfg, Config configCopy) {
        super(cfg, configCopy);
        this.cfg = cfg;
        if (cfg.getElastiCacheClient() != null) {
            elastiCacheClient = cfg.getElastiCacheClient();
            managedClient = false;
        } else {
            elastiCacheClient = createClient(cfg);
            managedClient = true;
        }

        long configuredInterval = Math.max(1L, cfg.getDiscoveryInterval());
        redisMonitorIntervalMs = Math.min(configuredInterval, REDIS_MONITOR_INTERVAL_MS);
        long forcedIntervalMs = Math.max(configuredInterval, MIN_FORCED_AWS_REFRESH_INTERVAL_MS);
        long jitterUpperBoundMs = Math.max(1L, forcedIntervalMs / 4);
        long randomExtraMs = ThreadLocalRandom.current().nextLong(jitterUpperBoundMs + 1);
        forcedAwsRefreshIntervalNanos = TimeUnit.MILLISECONDS.toNanos(forcedIntervalMs + randomExtraMs);
    }

    private ElastiCacheClient createClient(AwsReplicatedServersConfig cfg) {
        if (cfg.getRegion() == null || cfg.getRegion().isEmpty()) {
            throw new IllegalArgumentException("AWS region must be defined");
        }
        ElastiCacheClientBuilder builder = ElastiCacheClient.builder()
                .region(Region.of(cfg.getRegion()));

        AwsCredentialsProvider credentialsProvider = cfg.getCredentialsProvider();
        if (credentialsProvider != null) {
            builder.credentialsProvider(credentialsProvider);
        }
        if (cfg.getEndpointOverride() != null) {
            builder.endpointOverride(cfg.getEndpointOverride());
        }
        return builder.build();
    }

    @Override
    protected MasterSlaveServersConfig create(BaseMasterSlaveServersConfig<?> cfg) {
        MasterSlaveServersConfig res = super.create(cfg);
        res.setDatabase(((AwsReplicatedServersConfig) cfg).getDatabase());
        return res;
    }

    @Override
    public void doConnect(Function<RedisURI, String> hostnameMapper) {
        AwsTopologyView topologyView = describeTopology(true, false, 0);
        AwsTopology topology = topologyView.topology;
        if (topology.master == null) {
            throw new RedisConnectionException("AWS ElastiCache replication group "
                    + cfg.getReplicationGroupId() + " doesn't expose a primary endpoint");
        }

        Set<String> replicaSnapshot = replicaSet(topology.replicas);
        updateConfigAddresses(topology);
        currentMaster.set(topology.master);
        currentReplicas.set(replicaSnapshot);

        log.info("Initial AWS topology master {} replicas {}", topology.master, formatReplicaSet(replicaSnapshot));

        super.doConnect(hostnameMapper);

        lastAwsRefreshTimeNanos = topologyView.updatedAtNanos;
        scheduleRefresh();
    }

    private void scheduleRefresh() {
        if (serviceManager.isShuttingDown()) {
            return;
        }

        monitorFuture = serviceManager.newTimeout(timeout -> {
            if (serviceManager.isShuttingDown()) {
                return;
            }

            refreshTopology()
                    .whenComplete((r, e) -> {
                        if (e != null) {
                            log.error("Unable to refresh AWS replication group {}", targetDescription(), e);
                        }
                        scheduleRefresh();
                    });
        }, redisMonitorIntervalMs, TimeUnit.MILLISECONDS);
    }

    protected CompletableFuture<Void> refreshTopology() {
        if (serviceManager.isShuttingDown()) {
            return CompletableFuture.completedFuture(null);
        }

        Executor executor = serviceManager.getExecutor();
        return pollRedisTopology()
                .exceptionally(e -> {
                    log.warn("Unable to poll Redis topology for {}. Falling back to AWS metadata.", targetDescription(), e);
                    return true;
                })
                .thenCompose(needAws -> {
                    boolean forced = isAwsRefreshDue();
                    if (!needAws && !forced) {
                        return CompletableFuture.completedFuture(null);
                    }

                    if (needAws) {
                        log.debug("Redis topology change detected, refreshing AWS replication details for {}", targetDescription());
                    } else {
                        log.debug("Forcing AWS replication metadata refresh for {}", targetDescription());
                    }

                    long lastApplied = lastAwsRefreshTimeNanos;
                    boolean requireFresh = needAws || forced;
                    boolean bypassCache = forced;
                    CompletableFuture<AwsTopologyView> updateFuture = CompletableFuture
                            .supplyAsync(() -> describeTopology(requireFresh, bypassCache, lastApplied), executor)
                            .thenCompose(view -> applyTopology(view.topology)
                                    .thenApply(v -> view))
                            .whenComplete((view, ex) -> {
                                if (ex != null) {
                                    log.error("Failed to update AWS replication topology for {}", targetDescription(), ex);
                                } else if (view != null) {
                                    lastAwsRefreshTimeNanos = view.updatedAtNanos;
                                    redisInfoSnapshot.set(null);
                                }
                            });
                    return updateFuture.thenApply(v -> null);
                });
    }

    protected CompletableFuture<Boolean> pollRedisTopology() {
        if (serviceManager.isShuttingDown()) {
            return CompletableFuture.completedFuture(false);
        }

        MasterSlaveEntry entry = getEntry(singleSlotRange.getStartSlot());
        if (entry == null || !entry.isInit()) {
            return CompletableFuture.completedFuture(false);
        }

        CompletableFuture<RedisConnection> connectionFuture = entry.connectionWriteOp(RedisCommands.INFO_REPLICATION);
        return connectionFuture.thenCompose(connection ->
                connection.async(1, config.getRetryDelay(), config.getTimeout(),
                                StringCodec.INSTANCE, RedisCommands.INFO_REPLICATION)
                        .handle((info, throwable) -> {
                            try {
                                if (throwable != null) {
                                    log.debug("Unable to read replication info from {}", connection.getRedisClient().getAddr(), throwable);
                                    return true;
                                }
                                if (info == null) {
                                    return false;
                                }
                                RedisReplicationState newState = RedisReplicationState.from(info);
                                RedisReplicationState previous = redisInfoSnapshot.getAndSet(newState);
                                boolean needAws = shouldTriggerAws(previous, newState);
                                if (needAws && log.isDebugEnabled()) {
                                    log.debug("Redis INFO replication drift detected. Previous: {}, Current: {}", previous, newState);
                                }
                                return needAws;
                            } finally {
                                entry.releaseWrite(connection);
                            }
                        }).toCompletableFuture()
        ).exceptionally(e -> {
            log.debug("Unable to poll Redis topology for {}", targetDescription(), e);
            return true;
        });
    }

    protected boolean isAwsRefreshDue() {
        if (lastAwsRefreshTimeNanos == 0) {
            return true;
        }
        return System.nanoTime() - lastAwsRefreshTimeNanos >= forcedAwsRefreshIntervalNanos;
    }

    private boolean shouldTriggerAws(RedisReplicationState previous, RedisReplicationState current) {
        if (current == null) {
            return false;
        }
        if (!current.isMaster()) {
            return true;
        }
        if (previous == null) {
            return false;
        }
        if (!previous.isMaster()) {
            return true;
        }
        return !previous.getSlaveEndpoints().equals(current.getSlaveEndpoints());
    }

    protected AwsTopologyView describeTopology(boolean requireFresh, boolean bypassCache, long lastAppliedTimeNanos) {
        AwsTopologyKey key = topologyKey();
        if (!bypassCache) {
            long now = System.nanoTime();
            AwsTopologyView cached = SHARED_TOPOLOGY_CACHE.get(key);
            if (cached != null) {
                long age = now - cached.updatedAtNanos;
                if (requireFresh) {
                    if (cached.updatedAtNanos > lastAppliedTimeNanos
                            && age <= forcedAwsRefreshIntervalNanos) {
                        return cached;
                    }
                } else if (age <= forcedAwsRefreshIntervalNanos) {
                    return cached;
                }
            }
        }
        return fetchAndCacheTopology(key);
    }

    private AwsTopologyView fetchAndCacheTopology(AwsTopologyKey key) {
        while (true) {
            CompletableFuture<AwsTopologyView> existing = SHARED_REFRESHES.get(key);
            if (existing != null) {
                try {
                    return existing.get(MAX_SHARED_REFRESH_WAIT_MS, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RedisConnectionException("Interrupted while waiting for AWS topology refresh for " + key, e);
                } catch (ExecutionException e) {
                    Throwable cause;
                    if (e.getCause() != null) {
                        cause = e.getCause();
                    } else {
                        cause = e;
                    }
                    throw new RedisConnectionException("Unable to refresh AWS topology for " + key, cause);
                } catch (TimeoutException e) {
                    if (SHARED_REFRESHES.remove(key, existing)) {
                        log.warn("Timed out waiting for AWS topology refresh for {}. Retrying.", key);
                    }
                    continue;
                }
            }
            CompletableFuture<AwsTopologyView> future = new CompletableFuture<>();
            if (SHARED_REFRESHES.putIfAbsent(key, future) != null) {
                continue;
            }
            try {
                AwsTopology topology = loadAwsTopology();
                AwsTopologyView view = new AwsTopologyView(topology, System.nanoTime());
                SHARED_TOPOLOGY_CACHE.put(key, view);
                future.complete(view);
                return view;
            } catch (Exception e) {
                future.completeExceptionally(e);
                throw e;
            } finally {
                SHARED_REFRESHES.remove(key, future);
            }
        }
    }

    private AwsTopologyKey topologyKey() {
        String endpointOverride = null;
        if (cfg.getEndpointOverride() != null) {
            endpointOverride = cfg.getEndpointOverride().toString();
        }
        return new AwsTopologyKey(cfg.getRegion(),
                endpointOverride,
                cfg.getReplicationGroupId(),
                normalizeEndpoint(cfg.getDiscoveryEndpoint()),
                cfg.isUseSsl(),
                topologyClientIdentity());
    }

    private String topologyClientIdentity() {
        if (cfg.getElastiCacheClient() != null) {
            return "client@" + System.identityHashCode(cfg.getElastiCacheClient());
        }
        AwsCredentialsProvider provider = cfg.getCredentialsProvider();
        if (provider != null) {
            return provider.getClass().getName() + '@' + System.identityHashCode(provider);
        }
        return "managed";
    }

    protected ReplicationGroup resolveReplicationGroup() {
        String replicationGroupId = cfg.getReplicationGroupId();
        if (replicationGroupId != null && !replicationGroupId.isEmpty()) {
            DescribeReplicationGroupsResponse response = elastiCacheClient.describeReplicationGroups(
                    DescribeReplicationGroupsRequest.builder()
                            .replicationGroupId(replicationGroupId)
                            .build());
            if (response.replicationGroups().isEmpty()) {
                throw new RedisConnectionException("AWS replication group " + replicationGroupId + " not found");
            }
            return response.replicationGroups().get(0);
        }

        DescribeReplicationGroupsResponse response = elastiCacheClient.describeReplicationGroups(
                DescribeReplicationGroupsRequest.builder().build());
        List<ReplicationGroup> groups = response.replicationGroups();
        if (groups.isEmpty()) {
            throw new RedisConnectionException("No AWS replication groups found");
        }

        String endpoint = normalizeEndpoint(cfg.getDiscoveryEndpoint());
        if (endpoint != null) {
            for (ReplicationGroup group : groups) {
                if (matchesGroupEndpoint(group, endpoint)) {
                    return group;
                }
            }
            throw new RedisConnectionException("AWS replication group with discovery endpoint " + endpoint + " not found");
        }

        if (groups.size() == 1) {
            return groups.get(0);
        }

        throw new RedisConnectionException("Multiple AWS replication groups found. Specify replicationGroupId or discoveryEndpoint.");
    }

    protected AwsTopology loadAwsTopology() {
        ReplicationGroup group = resolveReplicationGroup();
        if (group == null) {
            throw new RedisConnectionException("AWS replication group was not found");
        }

        List<NodeGroup> nodeGroups = group.nodeGroups();

        Map<String, Role> memberRoles = new HashMap<>();
        if (nodeGroups != null) {
            for (NodeGroup nodeGroup : nodeGroups) {
                List<NodeGroupMember> members = nodeGroup.nodeGroupMembers();
                if (members == null) {
                    continue;
                }
                for (NodeGroupMember member : members) {
                    if (member.cacheClusterId() == null) {
                        continue;
                    }
                    memberRoles.put(member.cacheClusterId(), Role.from(member.currentRole()));
                }
            }
        }

        List<RedisURI> replicas = new ArrayList<>();
        RedisURI master = null;

        for (Map.Entry<String, Role> entry : memberRoles.entrySet()) {
            CacheCluster cluster = describeCluster(entry.getKey());
            if (cluster == null) {
                continue;
            }

            RedisURI uri = nodeEndpoint(cluster);
            if (uri == null) {
                continue;
            }

            if (entry.getValue() == Role.PRIMARY) {
                master = uri;
            } else {
                replicas.add(uri);
            }
        }

        if (master == null && nodeGroups != null) {
            for (NodeGroup nodeGroup : nodeGroups) {
                RedisURI uri = toRedisURI(nodeGroup.primaryEndpoint());
                if (uri != null) {
                    master = uri;
                    break;
                }
            }
        }

        if (master != null) {
            RedisURI finalMaster = master;
            replicas.removeIf(uri -> uri.equals(finalMaster));
        }

        return new AwsTopology(master, replicas);
    }

    private CacheCluster describeCluster(String clusterId) {
        DescribeCacheClustersResponse response = elastiCacheClient.describeCacheClusters(
                DescribeCacheClustersRequest.builder()
                        .cacheClusterId(clusterId)
                        .showCacheNodeInfo(true)
                        .build());

        if (response.cacheClusters().isEmpty()) {
            log.warn("AWS cluster {} is not available", clusterId);
            return null;
        }

        CacheCluster cluster = response.cacheClusters().get(0);
        if (cluster.cacheClusterStatus() != null
                && !"available".equalsIgnoreCase(cluster.cacheClusterStatus())) {
            log.debug("Skipping cluster {} with status {}", clusterId, cluster.cacheClusterStatus());
            return null;
        }
        return cluster;
    }

    private RedisURI nodeEndpoint(CacheCluster cluster) {
        List<CacheNode> cacheNodes = cluster.cacheNodes();
        if (cacheNodes == null || cacheNodes.isEmpty()) {
            return null;
        }
        Endpoint endpoint = cacheNodes.get(0).endpoint();
        return toRedisURI(endpoint);
    }

    private RedisURI toRedisURI(Endpoint endpoint) {
        if (endpoint == null
                || endpoint.address() == null
                || endpoint.port() == null) {
            return null;
        }
        String scheme;
        if (cfg.isUseSsl()) {
            scheme = "rediss";
        } else {
            scheme = "redis";
        }
        return new RedisURI(scheme, endpoint.address(), endpoint.port());
    }

    protected CompletableFuture<Void> applyTopology(AwsTopology topology) {
        if (topology.master == null) {
            return CompletableFuture.completedFuture(null);
        }

        Set<String> desiredReplicaSet = replicaSet(topology.replicas);
        Set<String> previousReplicaSet = currentReplicas.getAndSet(desiredReplicaSet);
        if (!desiredReplicaSet.equals(previousReplicaSet)) {
            log.info("Replica set changed from {} to {}", formatReplicaSet(previousReplicaSet), formatReplicaSet(desiredReplicaSet));
        }
        updateConfigAddresses(topology);

        List<CompletableFuture<Void>> tasks = new ArrayList<>();
        MasterSlaveEntry entry = getEntry(singleSlotRange.getStartSlot());

        RedisURI desiredMaster = topology.master;
        RedisURI current = currentMaster.get();
        if (current == null || !current.equals(desiredMaster)) {
            if (current != null) {
                log.info("Detected AWS primary change from {} to {}", current, desiredMaster);
            } else {
                log.info("Detected AWS primary {}", desiredMaster);
            }
            final RedisURI previousMaster = current;
            CompletableFuture<RedisClient> changeFuture = changeMaster(singleSlotRange.getStartSlot(), desiredMaster);
            CompletableFuture<Void> masterTask = changeFuture.thenCompose(client -> {
                currentMaster.set(desiredMaster);
                if (previousMaster != null && !previousMaster.equals(desiredMaster)) {
                    log.info("Primary node switched from {} to {}", previousMaster, desiredMaster);
                } else {
                    log.info("Primary node set to {}", desiredMaster);
                }
                return checkMasterIp(desiredMaster);
            });
            masterTask.whenComplete((v, e) -> {
                if (e != null) {
                    log.error("Unable to switch master to {}", desiredMaster, e);
                }
            });
            tasks.add(masterTask);
        } else {
            tasks.add(checkMasterIp(desiredMaster));
        }

        for (RedisURI replica : topology.replicas) {
            CompletableFuture<Void> ensureTask = ensureReplica(entry, replica);
            tasks.add(ensureTask);
        }

        removeMissingReplicas(entry, topology.replicas);

        if (tasks.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.allOf(tasks.toArray(new CompletableFuture[0]));
    }

    protected CompletableFuture<Void> ensureReplica(MasterSlaveEntry entry, RedisURI uri) {
        if (uri == null) {
            return CompletableFuture.completedFuture(null);
        }

        if (!entry.hasSlave(uri)) {
            CompletableFuture<Void> addFuture = entry.addSlave(uri, uri.getHost());
            addFuture.whenComplete((v, e) -> {
                if (e != null) {
                    log.error("Unable to add replica {}", uri, e);
                } else {
                    log.info("Replica {} added", uri);
                }
            });
            return addFuture.thenCompose(v -> checkReplicaIp(uri));
        }

        CompletableFuture<Boolean> reactivateFuture = entry.slaveUpAsync(uri);
        CompletableFuture<Void> result = reactivateFuture.thenAccept(restored -> {
            if (Boolean.TRUE.equals(restored)) {
                log.info("Replica {} is up", uri);
            }
        });
        return result.thenCompose(v -> checkReplicaIp(uri));
    }

    private String normalizeEndpoint(String endpoint) {
        if (endpoint == null) {
            return null;
        }
        String value = endpoint.trim();
        if (value.isEmpty()) {
            return null;
        }
        if (value.contains("://")) {
            try {
                return new RedisURI(value).getHost();
            } catch (Exception e) {
                return value;
            }
        }
        int colonIndex = value.indexOf(':');
        if (colonIndex >= 0) {
            return value.substring(0, colonIndex);
        }
        return value;
    }

    private boolean matchesGroupEndpoint(ReplicationGroup group, String host) {
        if (group.configurationEndpoint() != null && matchesEndpoint(group.configurationEndpoint(), host)) {
            return true;
        }
        List<NodeGroup> nodeGroups = group.nodeGroups();
        if (nodeGroups != null) {
            for (NodeGroup nodeGroup : nodeGroups) {
                if (matchesEndpoint(nodeGroup.primaryEndpoint(), host)) {
                    return true;
                }
                List<NodeGroupMember> members = nodeGroup.nodeGroupMembers();
                if (members != null) {
                    for (NodeGroupMember member : members) {
                        if (matchesEndpoint(member.readEndpoint(), host)) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    private boolean matchesEndpoint(Endpoint endpoint, String host) {
        if (endpoint == null || endpoint.address() == null) {
            return false;
        }
        return endpoint.address().equalsIgnoreCase(host);
    }

    private String targetDescription() {
        if (cfg.getReplicationGroupId() != null && !cfg.getReplicationGroupId().isEmpty()) {
            return cfg.getReplicationGroupId();
        }
        if (cfg.getDiscoveryEndpoint() != null && !cfg.getDiscoveryEndpoint().isEmpty()) {
            return cfg.getDiscoveryEndpoint();
        }
        return "unknown";
    }

    protected void removeMissingReplicas(MasterSlaveEntry entry, Collection<RedisURI> desiredReplicas) {
        Set<String> desired = desiredReplicas.stream()
                .map(RedisURI::toString)
                .collect(Collectors.toSet());

        for (ClientConnectionsEntry clientEntry : entry.getAllEntries()) {
            if (clientEntry.getNodeType() != NodeType.SLAVE) {
                continue;
            }
            RedisURI address = clientEntry.getClient().getConfig().getAddress();
            if (desired.contains(address.toString())) {
                continue;
            }
            if (entry.slaveDown(address)) {
                disconnectNode(address);
                log.info("Replica {} removed (no longer returned by AWS)", address);
            }
        }
    }

    protected CompletableFuture<Void> checkMasterIp(RedisURI masterUri) {
        return serviceManager.resolveIP(masterUri)
                .thenAccept(resolved -> {
                    MasterSlaveEntry entry = getEntry(singleSlotRange.getStartSlot());
                    RedisClient client = entry.getClient();
                    InetSocketAddress addr = client.getAddr();
                    if (!resolved.equals(addr)) {
                        String previous;
                        if (addr.getAddress() != null) {
                            previous = addr.getAddress().getHostAddress();
                        } else {
                            previous = addr.getHostString();
                        }
                        disconnectNode(masterUri);
                        log.info("Master hostname {} changed IP from {} to {}", masterUri,
                                previous, resolved.getHost());
                    }
                })
                .exceptionally(e -> {
                    log.error("Unable to resolve master {}", masterUri, e);
                    return null;
                });
    }

    protected CompletableFuture<Void> checkReplicaIp(RedisURI replicaUri) {
        return serviceManager.resolveIP(replicaUri)
                .thenAccept(resolved -> {
                    MasterSlaveEntry entry = getEntry(singleSlotRange.getStartSlot());
                    ClientConnectionsEntry clientEntry = entry.getEntry(replicaUri);
                    if (clientEntry == null) {
                        return;
                    }
                    InetSocketAddress currentAddr = clientEntry.getClient().getAddr();
                    if (!resolved.equals(currentAddr)) {
                        String previous;
                        if (currentAddr.getAddress() != null) {
                            previous = currentAddr.getAddress().getHostAddress();
                        } else {
                            previous = currentAddr.getHostString();
                        }
                        disconnectNode(replicaUri);
                        log.info("Replica hostname {} changed IP from {} to {}", replicaUri,
                                previous, resolved.getHost());
                    }
                })
                .exceptionally(e -> {
                    log.error("Unable to resolve replica {}", replicaUri, e);
                    return null;
                });
    }

    protected void updateConfigAddresses(AwsTopology topology) {
        config.setMasterAddress(topology.master.toString());
        Set<String> slaveAddresses = topology.replicas.stream()
                .map(RedisURI::toString)
                .collect(Collectors.toCollection(HashSet::new));
        config.setSlaveAddresses(slaveAddresses);
    }

    @Override
    public void shutdown(long quietPeriod, long timeout, TimeUnit unit) {
        if (monitorFuture != null) {
            monitorFuture.cancel();
        }
        closeNodeConnections();
        if (managedClient) {
            elastiCacheClient.close();
        }
        super.shutdown(quietPeriod, timeout, unit);
    }

    private static final class AwsTopologyView {

        private final AwsTopology topology;
        private final long updatedAtNanos;

        private AwsTopologyView(AwsTopology topology, long updatedAtNanos) {
            this.topology = topology;
            this.updatedAtNanos = updatedAtNanos;
        }
    }

    private static final class AwsTopologyKey {

        private final String region;
        private final String endpointOverride;
        private final String replicationGroupId;
        private final String discoveryEndpoint;
        private final boolean useSsl;
        private final String clientIdentity;

        private AwsTopologyKey(String region, String endpointOverride,
                               String replicationGroupId, String discoveryEndpoint,
                               boolean useSsl, String clientIdentity) {
            this.region = region;
            this.endpointOverride = endpointOverride;
            this.replicationGroupId = replicationGroupId;
            this.discoveryEndpoint = discoveryEndpoint;
            this.useSsl = useSsl;
            this.clientIdentity = clientIdentity;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AwsTopologyKey that = (AwsTopologyKey) o;
            return useSsl == that.useSsl
                    && Objects.equals(region, that.region)
                    && Objects.equals(endpointOverride, that.endpointOverride)
                    && Objects.equals(replicationGroupId, that.replicationGroupId)
                    && Objects.equals(discoveryEndpoint, that.discoveryEndpoint)
                    && Objects.equals(clientIdentity, that.clientIdentity);
        }

        @Override
        public int hashCode() {
            return Objects.hash(region, endpointOverride, replicationGroupId, discoveryEndpoint, useSsl, clientIdentity);
        }

        @Override
        public String toString() {
            return "AwsTopologyKey{" +
                    "region='" + region + '\'' +
                    ", groupId='" + replicationGroupId + '\'' +
                    ", discoveryEndpoint='" + discoveryEndpoint + '\'' +
                    ", endpointOverride='" + endpointOverride + '\'' +
                    ", useSsl=" + useSsl +
                    ", clientIdentity='" + clientIdentity + '\'' +
                    '}';
        }
    }

    private Set<String> replicaSet(List<RedisURI> replicas) {
        if (replicas == null || replicas.isEmpty()) {
            return Collections.emptySet();
        }
        Set<String> result = new HashSet<>(replicas.size());
        for (RedisURI replica : replicas) {
            result.add(replica.toString());
        }
        return Collections.unmodifiableSet(result);
    }

    private String formatReplicaSet(Set<String> replicas) {
        if (replicas == null || replicas.isEmpty()) {
            return "[]";
        }
        return replicas.stream()
                .sorted()
                .collect(Collectors.joining(", ", "[", "]"));
    }

    static final class RedisReplicationState {

        private final boolean master;
        private final String masterEndpoint;
        private final Set<String> slaveEndpoints;

        private RedisReplicationState(boolean master, String masterEndpoint, Set<String> slaveEndpoints) {
            this.master = master;
            this.masterEndpoint = masterEndpoint;
            this.slaveEndpoints = slaveEndpoints;
        }

        static RedisReplicationState from(Object info) {
            if (!(info instanceof Map)) {
                return null;
            }
            Map<?, ?> data = (Map<?, ?>) info;
            String role = asString(data.get("role"));
            boolean isMaster = "master".equalsIgnoreCase(role);
            if (isMaster) {
                Set<String> replicas = new TreeSet<>();
                int index = 0;
                while (true) {
                    Object raw = data.get("slave" + index);
                    if (raw == null) {
                        break;
                    }
                    String endpoint = parseEndpoint(asString(raw));
                    if (endpoint != null) {
                        replicas.add(endpoint);
                    }
                    index++;
                }
                Set<String> view = Collections.emptySet();
                if (!replicas.isEmpty()) {
                    view = Collections.unmodifiableSet(replicas);
                }
                return new RedisReplicationState(true, null, view);
            }

            String masterHost = asString(data.get("master_host"));
            String masterPort = asString(data.get("master_port"));
            String endpoint = null;
            if (masterHost != null && !masterHost.isEmpty()) {
                if (masterPort != null && !masterPort.isEmpty()) {
                    endpoint = masterHost + ":" + masterPort;
                } else {
                    endpoint = masterHost;
                }
            }
            return new RedisReplicationState(false, endpoint, Collections.emptySet());
        }

        boolean isMaster() {
            return master;
        }

        Set<String> getSlaveEndpoints() {
            return slaveEndpoints;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RedisReplicationState that = (RedisReplicationState) o;
            return master == that.master
                    && Objects.equals(masterEndpoint, that.masterEndpoint)
                    && Objects.equals(slaveEndpoints, that.slaveEndpoints);
        }

        @Override
        public int hashCode() {
            return Objects.hash(master, masterEndpoint, slaveEndpoints);
        }

        @Override
        public String toString() {
            if (master) {
                return "RedisReplicationState{role=master, replicas=" + slaveEndpoints + '}';
            }
            return "RedisReplicationState{role=slave, masterEndpoint='" + masterEndpoint + "'}";
        }

        private static String parseEndpoint(String definition) {
            if (definition == null || definition.isEmpty()) {
                return null;
            }
            String ip = null;
            String port = null;
            String[] parts = definition.split(",");
            for (String part : parts) {
                int idx = part.indexOf('=');
                if (idx <= 0) {
                    continue;
                }
                String key = part.substring(0, idx);
                String value = part.substring(idx + 1);
                if ("ip".equalsIgnoreCase(key)) {
                    ip = value;
                } else if ("port".equalsIgnoreCase(key)) {
                    port = value;
                }
            }
            if (ip == null || ip.isEmpty()) {
                return null;
            }
            if (port != null && !port.isEmpty()) {
                return ip + ":" + port;
            }
            return ip;
        }

        private static String asString(Object value) {
            if (value == null) {
                return null;
            }
            if (value instanceof CharSequence) {
                CharSequence seq = (CharSequence) value;
                if (seq.length() == 0) {
                    return null;
                }
                return seq.toString();
            }
            return Objects.toString(value, null);
        }
    }

    private enum Role {
        PRIMARY,
        REPLICA;

        static Role from(Object value) {
            if (value == null) {
                return REPLICA;
            }
            String text = Objects.toString(value);
            if ("primary".equalsIgnoreCase(text) || "master".equalsIgnoreCase(text)) {
                return PRIMARY;
            }
            return REPLICA;
        }
    }

    static class AwsTopology {

        private final RedisURI master;

        private final List<RedisURI> replicas;

        AwsTopology(RedisURI master, List<RedisURI> replicas) {
            this.master = master;
            this.replicas = replicas;
        }
    }
}
