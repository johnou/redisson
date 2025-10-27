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
package org.redisson.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.elasticache.ElastiCacheClient;

import java.net.URI;

/**
 * Configuration for AWS ElastiCache replication groups with dynamic node discovery.
 *
 * @author Johno Crawford (johno@sulake.com)
 */
public class AwsReplicatedServersConfig extends BaseMasterSlaveServersConfig<AwsReplicatedServersConfig> {

    /**
     * Replication group identifier. Either this or {@link #setDiscoveryEndpoint(String)} must be provided.
     */
    private String replicationGroupId;

    /**
     * Discovery endpoint hostname used to select a replication group when id isn't supplied.
     */
    private String discoveryEndpoint;

    /**
     * AWS region where replication group is deployed.
     */
    private String region;

    /**
     * Interval in milliseconds between topology refresh calls to AWS API.
     */
    private int discoveryInterval = 5_000;

    /**
     * Database index used for Redis connection
     */
    private int database = 0;

    /**
     * Use rediss scheme when connecting to endpoints.
     */
    private boolean useSsl;

    @JsonIgnore
    private AwsCredentialsProvider credentialsProvider;

    @JsonIgnore
    private URI endpointOverride;

    @JsonIgnore
    private ElastiCacheClient elastiCacheClient;

    public AwsReplicatedServersConfig() {
    }

    AwsReplicatedServersConfig(AwsReplicatedServersConfig config) {
        super(config);
        setReplicationGroupId(config.getReplicationGroupId());
        setDiscoveryEndpoint(config.getDiscoveryEndpoint());
        setRegion(config.getRegion());
        setDiscoveryInterval(config.getDiscoveryInterval());
        setDatabase(config.getDatabase());
        setUseSsl(config.isUseSsl());
        setCredentialsProvider(config.getCredentialsProvider());
        setEndpointOverride(config.getEndpointOverride());
        setElastiCacheClient(config.getElastiCacheClient());
    }

    public String getReplicationGroupId() {
        return replicationGroupId;
    }

    public AwsReplicatedServersConfig setReplicationGroupId(String replicationGroupId) {
        this.replicationGroupId = replicationGroupId;
        return this;
    }

    public String getDiscoveryEndpoint() {
        return discoveryEndpoint;
    }

    /**
     * Sets the primary/discovery endpoint Hostname. Used to autodetect the replication group when
     * {@link #setReplicationGroupId(String)} isn't provided.
     *
     * @param discoveryEndpoint hostname or redis(s):// URI
     * @return config
     */
    public AwsReplicatedServersConfig setDiscoveryEndpoint(String discoveryEndpoint) {
        this.discoveryEndpoint = discoveryEndpoint;
        return this;
    }

    public String getRegion() {
        return region;
    }

    public AwsReplicatedServersConfig setRegion(String region) {
        this.region = region;
        return this;
    }

    public int getDiscoveryInterval() {
        return discoveryInterval;
    }

    public AwsReplicatedServersConfig setDiscoveryInterval(int discoveryInterval) {
        this.discoveryInterval = discoveryInterval;
        return this;
    }

    public int getDatabase() {
        return database;
    }

    public AwsReplicatedServersConfig setDatabase(int database) {
        this.database = database;
        return this;
    }

    public boolean isUseSsl() {
        return useSsl;
    }

    public AwsReplicatedServersConfig setUseSsl(boolean useSsl) {
        this.useSsl = useSsl;
        return this;
    }

    public AwsCredentialsProvider getCredentialsProvider() {
        return credentialsProvider;
    }

    public AwsReplicatedServersConfig setCredentialsProvider(AwsCredentialsProvider credentialsProvider) {
        this.credentialsProvider = credentialsProvider;
        return this;
    }

    public URI getEndpointOverride() {
        return endpointOverride;
    }

    public AwsReplicatedServersConfig setEndpointOverride(URI endpointOverride) {
        this.endpointOverride = endpointOverride;
        return this;
    }

    public ElastiCacheClient getElastiCacheClient() {
        return elastiCacheClient;
    }

    public AwsReplicatedServersConfig setElastiCacheClient(ElastiCacheClient elastiCacheClient) {
        this.elastiCacheClient = elastiCacheClient;
        return this;
    }
}
