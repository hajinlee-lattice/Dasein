package com.latticeengines.aws.elasticache.impl;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.elasticache.AmazonElastiCache;
import com.amazonaws.services.elasticache.AmazonElastiCacheClientBuilder;
import com.amazonaws.services.elasticache.model.CacheCluster;
import com.amazonaws.services.elasticache.model.DescribeCacheClustersRequest;
import com.amazonaws.services.elasticache.model.DescribeCacheClustersResult;
import com.amazonaws.services.elasticache.model.DescribeReplicationGroupsRequest;
import com.amazonaws.services.elasticache.model.DescribeReplicationGroupsResult;
import com.amazonaws.services.elasticache.model.Endpoint;
import com.amazonaws.services.elasticache.model.NodeGroup;
import com.amazonaws.services.elasticache.model.ReplicationGroup;
import com.latticeengines.aws.elasticache.ElastiCacheService;

@Service("elastiCacheService")
public class ElastiCacheServiceImpl implements ElastiCacheService {

    private AmazonElastiCache client;

    private String clusterName;

    @Inject
    public ElastiCacheServiceImpl(AWSCredentials awsCredentials, @Value("${aws.region}") String region,
            @Value("${aws.elasticache.cluster.name}") String clusterName) {
        this.client = AmazonElastiCacheClientBuilder.standard() //
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))//
                .withRegion(region)//
                .build();
        this.clusterName = clusterName;
    }

    @Override
    public List<String> getDistributedCacheNodeAddresses() {
        return getReplicatedNodeAddrs(clusterName);
    }

    private List<String> getReplicatedNodeAddrs(String groupName) {
        List<String> addrPortInfo = new ArrayList<>();

        DescribeReplicationGroupsRequest drgRequest = new DescribeReplicationGroupsRequest();
        drgRequest.setReplicationGroupId(groupName);
        DescribeReplicationGroupsResult result = client.describeReplicationGroups(drgRequest);
        for (ReplicationGroup replicationGroup: result.getReplicationGroups()) {
            if (replicationGroup.getReplicationGroupId().equalsIgnoreCase(groupName)) {
                boolean encrypted = replicationGroup.getTransitEncryptionEnabled();
                NodeGroup nodeGroup = replicationGroup.getNodeGroups().get(0);
                Endpoint primaryEndpoint = nodeGroup.getPrimaryEndpoint();
                addrPortInfo.add(getFullAddr(primaryEndpoint, encrypted));
                for (String childCluster: replicationGroup.getMemberClusters()) {
                    addrPortInfo.add(getClusterNodeAddr(childCluster));
                }
            }
        }

        if (CollectionUtils.isEmpty(addrPortInfo)) {
            throw new IllegalArgumentException("Failed to get addresses for replication group named " + groupName);
        }

        return addrPortInfo;
    }

    private String getClusterNodeAddr(String clusterName) {
        String addr = "";

        DescribeCacheClustersRequest dccRequest = new DescribeCacheClustersRequest();
        dccRequest.setShowCacheNodeInfo(true);
        dccRequest.setCacheClusterId(clusterName);
        DescribeCacheClustersResult clusterResult = client.describeCacheClusters(dccRequest);

        for (CacheCluster cacheCluster : clusterResult.getCacheClusters()) {
            if (cacheCluster.getCacheClusterId().equals(clusterName)) {
                boolean encrypted = cacheCluster.getTransitEncryptionEnabled();
                addr = getFullAddr(cacheCluster.getCacheNodes().get(0).getEndpoint(), encrypted);
            }
        }

        if (StringUtils.isBlank(addr)) {
            throw new IllegalArgumentException("Cannot find node address for elastic cache cluster named " + clusterName);
        }

        return addr;
    }

    private static String getFullAddr(Endpoint endpoint, boolean encrypted) {
        String protocol = encrypted ? "rediss" : "redis";
        return String.format("%s://%s:%d", protocol, endpoint.getAddress(), endpoint.getPort());
    }

}
