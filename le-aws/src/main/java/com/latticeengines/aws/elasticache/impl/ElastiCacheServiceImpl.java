package com.latticeengines.aws.elasticache.impl;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.elasticache.AmazonElastiCache;
import com.amazonaws.services.elasticache.AmazonElastiCacheClientBuilder;
import com.amazonaws.services.elasticache.model.CacheCluster;
import com.amazonaws.services.elasticache.model.CacheNode;
import com.amazonaws.services.elasticache.model.DescribeCacheClustersRequest;
import com.amazonaws.services.elasticache.model.DescribeCacheClustersResult;
import com.latticeengines.aws.elasticache.ElastiCacheService;

@Component("elastiCacheService")
public class ElastiCacheServiceImpl implements ElastiCacheService {

    private AmazonElastiCache client;

    private String clusterName;

    @Autowired
    public ElastiCacheServiceImpl(AWSCredentials awsCredentials, @Value("${aws.region}") String region,
            @Value("${aws.elasticache.cluster.name}") String clusterName) {
        this.client = AmazonElastiCacheClientBuilder.standard() //
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))//
                .withRegion(region)//
                .build();
        this.clusterName = clusterName;
    }

    @Override
    public List<String> getNodeAddresses() {
        List<String> addrPortInfo = new ArrayList<>();

        DescribeCacheClustersRequest dccRequest = new DescribeCacheClustersRequest();
        dccRequest.setShowCacheNodeInfo(true);
        DescribeCacheClustersResult clusterResult = client.describeCacheClusters(dccRequest);

        for (CacheCluster cacheCluster : clusterResult.getCacheClusters()) {
            if (!cacheCluster.getReplicationGroupId().equals(clusterName)) {
                continue;
            }
            for (CacheNode cacheNode : cacheCluster.getCacheNodes()) {
                String addr = cacheNode.getEndpoint().getAddress();
                int port = cacheNode.getEndpoint().getPort();
                if (clusterName.contains("encrypted")) {
                    addrPortInfo.add(String.format("rediss://%s:%d", addr, port));
                } else {
                    addrPortInfo.add(String.format("redis://%s:%d", addr, port));
                }
            }
        }
        return addrPortInfo;
    }
}
