package com.latticeengines.aws.emr.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.ClusterState;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterResult;
import com.amazonaws.services.elasticmapreduce.model.Instance;
import com.amazonaws.services.elasticmapreduce.model.InstanceFleet;
import com.amazonaws.services.elasticmapreduce.model.InstanceFleetModifyConfig;
import com.amazonaws.services.elasticmapreduce.model.InstanceFleetType;
import com.amazonaws.services.elasticmapreduce.model.InstanceGroup;
import com.amazonaws.services.elasticmapreduce.model.InstanceGroupModifyConfig;
import com.amazonaws.services.elasticmapreduce.model.InstanceGroupType;
import com.amazonaws.services.elasticmapreduce.model.InvalidRequestException;
import com.amazonaws.services.elasticmapreduce.model.ListClustersRequest;
import com.amazonaws.services.elasticmapreduce.model.ListClustersResult;
import com.amazonaws.services.elasticmapreduce.model.ListInstanceFleetsRequest;
import com.amazonaws.services.elasticmapreduce.model.ListInstanceFleetsResult;
import com.amazonaws.services.elasticmapreduce.model.ListInstanceGroupsRequest;
import com.amazonaws.services.elasticmapreduce.model.ListInstanceGroupsResult;
import com.amazonaws.services.elasticmapreduce.model.ListInstancesRequest;
import com.amazonaws.services.elasticmapreduce.model.ListInstancesResult;
import com.amazonaws.services.elasticmapreduce.model.ModifyInstanceFleetRequest;
import com.amazonaws.services.elasticmapreduce.model.ModifyInstanceFleetResult;
import com.amazonaws.services.elasticmapreduce.model.ModifyInstanceGroupsRequest;
import com.amazonaws.services.elasticmapreduce.model.ModifyInstanceGroupsResult;
import com.amazonaws.services.identitymanagement.model.NoSuchEntityException;
import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.aws.emr.EMRService;
import com.latticeengines.common.exposed.util.RetryUtils;

@Service("emrService")
public class EMRServiceImpl implements EMRService {

    private static final Logger log = LoggerFactory.getLogger(EMRServiceImpl.class);
    private static final Map<String, String> masterIpCache = new ConcurrentHashMap<>();
    private static final Map<String, String> clusterIdCache = new ConcurrentHashMap<>();

    private AmazonElasticMapReduce emrClient;

    @Value("${aws.emr.cluster}")
    private String clusterName;

    @Value("${aws.region}")
    private String region;

    @Resource(name = "awsCredentials")
    private AWSCredentials awsCredentials;

    @Override
    public String getMasterIp(String clusterId) {
        if (!masterIpCache.containsKey(clusterId)) {
            synchronized (this) {
                if (!masterIpCache.containsKey(clusterId)) {
                    String masterIp = null;
                    AmazonElasticMapReduce emr = getEmr();
                    if (StringUtils.isNotBlank(clusterId)) {
                        RetryTemplate retryTemplate = RetryUtils.getRetryTemplate(10, null, //
                                Collections.singleton(NoSuchEntityException.class));
                        DescribeClusterResult cluster = retryTemplate.execute(context -> {
                            if (context.getRetryCount() > 0) {
                                log.info("(Attempt=" + (context.getRetryCount() + 1) + ") describe cluster " + clusterId);
                            }
                            return emr.describeCluster(new DescribeClusterRequest().withClusterId(clusterId));
                        });
                        String masterDNS = cluster.getCluster().getMasterPublicDnsName();
                        ListInstancesResult instances = retryTemplate.execute(context -> {
                            if (context.getRetryCount() > 0) {
                                log.info("(Attempt=" + (context.getRetryCount() + 1) + ") list instances in cluster " + clusterId);
                            }
                            return emr.listInstances(new ListInstancesRequest().withClusterId(clusterId));
                        });
                        for (Instance instance : instances.getInstances()) {
                            String instancePublicDNS = instance.getPublicDnsName();
                            String instancePrivateDNS = instance.getPrivateDnsName();
                            if (masterDNS.equals(instancePublicDNS) || masterDNS.equals(instancePrivateDNS)) {
                                masterIp = instance.getPrivateIpAddress();
                            }
                        }
                    }
                    if (StringUtils.isNotBlank(masterIp)) {
                        masterIpCache.putIfAbsent(clusterId, masterIp);
                    }
                }
            }
        }
        return masterIpCache.get(clusterId);
    }

    @Override
    public boolean isActive(String clusterId) {
        DescribeClusterResult cluster = describeCluster(clusterId);
        boolean active = false;
        if (cluster != null) {
            ClusterState state = ClusterState.fromValue(cluster.getCluster().getStatus().getState());
            active = Arrays.asList(ClusterState.RUNNING, ClusterState.WAITING).contains(state);
        }
        return active;
    }

    @Override
    public String getSqoopHostPort() {
        String clusterId = getClusterId(clusterName);
        return "http://" + getMasterIp(clusterId) + ":8081";
    }

    @Override
    public String getLogBucket(String clusterId) {
        DescribeClusterResult cluster = describeCluster(clusterId);
        if (cluster != null) {
            String logUri = cluster.getCluster().getLogUri();
            String bucket = logUri.split("://")[1];
            bucket = bucket.split("/")[0];
            return bucket;
        } else {
            return null;
        }
    }

    @Override
    public InstanceGroup getTaskGroup(String clusterId) {
        return getInstanceGroup(clusterId, InstanceGroupType.TASK);
    }

    @Override
    public InstanceGroup getCoreGroup(String clusterId) {
        return getInstanceGroup(clusterId, InstanceGroupType.CORE);
    }

    @Override
    public InstanceFleet getTaskFleet(String clusterId) {
        return getInstanceFleet(clusterId, InstanceFleetType.TASK);
    }

    @Override
    public InstanceFleet getCoreFleet(String clusterId) {
        return getInstanceFleet(clusterId, InstanceFleetType.CORE);
    }

    @Override
    public void scaleTaskGroup(String clusterId, InstanceGroup taskGrp, int targetCount) {
        AmazonElasticMapReduce emr = getEmr();
        InstanceGroupModifyConfig modifyConfig = new InstanceGroupModifyConfig()
                .withInstanceGroupId(taskGrp.getId())
                .withInstanceCount(targetCount);
        ModifyInstanceGroupsRequest request = //
                new ModifyInstanceGroupsRequest().withClusterId(clusterId).withInstanceGroups(modifyConfig);
        ModifyInstanceGroupsResult result = emr.modifyInstanceGroups(request);
        log.info("Sent emr scaling request, got response: " + result);
    }

    @Override
    public void scaleTaskFleet(String clusterId, InstanceFleet taskFleet, int targetOnDemandCount, int targetSpotCount) {
        AmazonElasticMapReduce emr = getEmr();
        InstanceFleetModifyConfig modifyConfig = new InstanceFleetModifyConfig()
                .withInstanceFleetId(taskFleet.getId())
                .withTargetOnDemandCapacity(targetOnDemandCount)
                .withTargetSpotCapacity(targetSpotCount);
        ModifyInstanceFleetRequest request = new ModifyInstanceFleetRequest().withClusterId(clusterId).withInstanceFleet(modifyConfig);
        ModifyInstanceFleetResult result = emr.modifyInstanceFleet(request);
        log.info("Sent emr scaling request, got response: " + result);
    }

    @Override
    public String getClusterId(String clusterName) {
        if (!clusterIdCache.containsKey(clusterName)) {
            synchronized (this) {
                if (!clusterIdCache.containsKey(clusterName)) {
                    ListClustersResult clustersResult = listClusters();
                    for (ClusterSummary summary : clustersResult.getClusters()) {
                        if (summary.getName().endsWith(clusterName)) {
                            log.info("Found an EMR cluster named " + summary.getName());
                            clusterIdCache.put(clusterName, summary.getId());
                        }
                    }
                }
            }
        }
        return clusterIdCache.get(clusterName);
    }

    public List<ClusterSummary> findClusters(Predicate<ClusterSummary> filter) {
        ListClustersResult clustersResult = listClusters();
        if (clustersResult != null) {
            return clustersResult.getClusters()
                    .stream().filter(filter).collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    private ListClustersResult listClusters() {
        AmazonElasticMapReduce emr = getEmr();
        RetryTemplate retryTemplate = RetryUtils.getExponentialBackoffRetryTemplate( //
                5, 5000L, 2.0D, null);
        return retryTemplate.execute(context -> {
            if (context.getRetryCount() > 0) {
                log.info(String.format("(attempt=%d) list emr clusters", context.getRetryCount() + 1));
            }
            ListClustersRequest request = new ListClustersRequest().withClusterStates(ClusterState.RUNNING,
                    ClusterState.WAITING);
            return emr.listClusters(request);
        });
    }

    @VisibleForTesting
    AmazonElasticMapReduce getEmr() {
        if (emrClient == null) {
            synchronized (this) {
                if (emrClient == null) {
                    emrClient = AmazonElasticMapReduceClientBuilder.standard() //
                            .withCredentials(new AWSStaticCredentialsProvider(awsCredentials)) //
                            .withRegion(Regions.fromName(region)) //
                            .build();
                }
            }
        }
        return emrClient;
    }

    private DescribeClusterResult describeCluster(String clusterId) {
        AmazonElasticMapReduce emr = getEmr();
        RetryTemplate retryTemplate = RetryUtils.getRetryTemplate(10, null, //
                Collections.singleton(NoSuchEntityException.class));
        DescribeClusterResult cluster;
        try {
            cluster = retryTemplate.execute(context -> //
                    emr.describeCluster(new DescribeClusterRequest().withClusterId(clusterId)));
        } catch (NoSuchEntityException | InvalidRequestException e) {
            log.warn("No cluster with id " + clusterId, e);
            return null;
        }
        return cluster;
    }

    private InstanceGroup getInstanceGroup(String clusterId, InstanceGroupType groupType) {
        AmazonElasticMapReduce emr = getEmr();
        RetryTemplate retryTemplate = RetryUtils.getRetryTemplate(5, null, //
                Arrays.asList(NoSuchEntityException.class, InvalidRequestException.class));
        try {
            ListInstanceGroupsResult result = retryTemplate.execute(context -> {
                ListInstanceGroupsRequest listGrpRequest = new ListInstanceGroupsRequest().withClusterId(clusterId);
                return emr.listInstanceGroups(listGrpRequest);
            });
            return result.getInstanceGroups().stream() //
                    .filter(grp -> //
                            grp.getRequestedInstanceCount() > 0 && groupType.name().equals(grp.getInstanceGroupType())) //
                    .findFirst().orElse(null);
        } catch (InvalidRequestException e) {
            if (e.getMessage().contains("mutually exclusive")) {
                // it is an instance fleet cluster
                return null;
            } else {
                throw e;
            }
        }
    }

    private InstanceFleet getInstanceFleet(String clusterId, InstanceFleetType fleetType) {
        AmazonElasticMapReduce emr = getEmr();
        RetryTemplate retryTemplate = RetryUtils.getRetryTemplate(5, null, //
                Arrays.asList(NoSuchEntityException.class, InvalidRequestException.class));
        try {
            ListInstanceFleetsResult result = retryTemplate.execute(context -> {
                ListInstanceFleetsRequest listGrpRequest = new ListInstanceFleetsRequest().withClusterId(clusterId);
                return emr.listInstanceFleets(listGrpRequest);
            });
            return result.getInstanceFleets().stream() //
                    .filter(grp -> //
                            Math.max(grp.getTargetOnDemandCapacity(), grp.getTargetSpotCapacity()) > 0 //
                                    && fleetType.name().equals(grp.getInstanceFleetType())) //
                    .findFirst().orElse(null);
        } catch (InvalidRequestException e) {
            if (e.getMessage().contains("mutually exclusive")) {
                // it is an instance group cluster
                return null;
            } else {
                throw e;
            }
        }
    }

}
