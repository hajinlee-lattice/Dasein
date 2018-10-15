package com.latticeengines.aws.emr.impl;

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
import com.amazonaws.services.elasticmapreduce.model.InstanceGroup;
import com.amazonaws.services.elasticmapreduce.model.InstanceGroupModifyConfig;
import com.amazonaws.services.elasticmapreduce.model.InstanceGroupType;
import com.amazonaws.services.elasticmapreduce.model.ListClustersRequest;
import com.amazonaws.services.elasticmapreduce.model.ListClustersResult;
import com.amazonaws.services.elasticmapreduce.model.ListInstanceGroupsRequest;
import com.amazonaws.services.elasticmapreduce.model.ListInstanceGroupsResult;
import com.amazonaws.services.elasticmapreduce.model.ListInstancesRequest;
import com.amazonaws.services.elasticmapreduce.model.ListInstancesResult;
import com.amazonaws.services.elasticmapreduce.model.ModifyInstanceGroupsRequest;
import com.amazonaws.services.elasticmapreduce.model.ModifyInstanceGroupsResult;
import com.latticeengines.aws.emr.EMRService;
import com.latticeengines.common.exposed.util.RetryUtils;

@Service("emrService")
public class EMRServiceImpl implements EMRService {

    private static final Logger log = LoggerFactory.getLogger(EMRServiceImpl.class);

    private AmazonElasticMapReduce emrClient;

    @Value("${aws.emr.cluster}")
    private String clusterName;

    @Value("${aws.region}")
    private String region;

    @Resource(name = "awsCredentials")
    private AWSCredentials awsCredentials;

    @Override
    public String getMasterIp() {
        return getMasterIp(clusterName);
    }

    @Override
    public String getMasterIp(String clusterName) {
        AmazonElasticMapReduce emr = getEmr();
        String masterIp = null;
        String clusterId = getClusterId(clusterName);
        if (StringUtils.isNotBlank(clusterId)) {
            DescribeClusterResult cluster = emr
                    .describeCluster(new DescribeClusterRequest().withClusterId(clusterId));
            String masterDNS = cluster.getCluster().getMasterPublicDnsName();
            ListInstancesResult instances = emr
                    .listInstances(new ListInstancesRequest().withClusterId(clusterId));
            for (Instance instance : instances.getInstances()) {
                String instancePublicDNS = instance.getPublicDnsName();
                String instancePrivateDNS = instance.getPrivateDnsName();
                if (masterDNS.equals(instancePublicDNS) || masterDNS.equals(instancePrivateDNS)) {
                    masterIp = instance.getPrivateIpAddress();
                    log.info("The private IP of master node in the cluster named " + clusterName + " is "
                            + masterIp);
                }
            }
        }
        return masterIp;
    }

    @Override
    public String getWebHdfsUrl() {
        return "http://" + getMasterIp() + ":50070/webhdfs/v1";
    }

    @Override
    public String getSqoopHostPort() {
        return "http://" + getMasterIp() + ":8081";
    }

    @Override
    public InstanceGroup getTaskGroup(String clusterName) {
        String clusterId = getClusterId(clusterName);
        if (StringUtils.isBlank(clusterId)) {
            log.info("Cannot find emrcluster named " + clusterName);
            return null;
        }
        AmazonElasticMapReduce emr = getEmr();
        RetryTemplate retryTemplate = RetryUtils.getRetryTemplate(5);
        ListInstanceGroupsResult result = retryTemplate.execute(context -> {
            ListInstanceGroupsRequest listGrpRequest = new ListInstanceGroupsRequest().withClusterId(clusterId);
            return emr.listInstanceGroups(listGrpRequest);
        });
        return result.getInstanceGroups().stream() //
                .filter(grp -> InstanceGroupType.TASK.name().equals(grp.getInstanceGroupType())) //
                .findFirst().orElse(null);
    }

    @Override
    public void scaleTaskGroup(String clusterName, int targetCount) {
        if (targetCount > 0) {
            InstanceGroup taskGrp = getTaskGroup(clusterName);
            if (taskGrp != null) {
                AmazonElasticMapReduce emr = getEmr();
                InstanceGroupModifyConfig modifyConfig = new InstanceGroupModifyConfig()
                        .withInstanceGroupId(taskGrp.getId())
                        .withInstanceCount(targetCount);
                ModifyInstanceGroupsRequest request = //
                        new ModifyInstanceGroupsRequest().withInstanceGroups(modifyConfig);
                ModifyInstanceGroupsResult result = emr.modifyInstanceGroups(request);
                log.info("Sent emr scaling request, got response: " + result);
            }
        } else {
            log.info("Illegal target count " + targetCount);
        }
    }

    private String getClusterId(String clusterName) {
        AmazonElasticMapReduce emr = getEmr();
        RetryTemplate retryTemplate = RetryUtils.getRetryTemplate(5);
        ListClustersResult clustersResult = retryTemplate.execute(context -> {
            ListClustersRequest request = new ListClustersRequest().withClusterStates(ClusterState.RUNNING,
                    ClusterState.WAITING);
            return emr.listClusters(request);
        });
        for (ClusterSummary summary : clustersResult.getClusters()) {
            if (summary.getName().endsWith(clusterName)) {
                log.info("Found an EMR cluster named " + summary.getName());
                return summary.getId();
            }
        }
        return null;
    }

    private AmazonElasticMapReduce getEmr() {
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

    private String getClusterId(AmazonElasticMapReduce emr, String clusterName) {
        RetryTemplate retryTemplate = RetryUtils.getRetryTemplate(5);
        ListClustersResult clustersResult = retryTemplate.execute(context -> {
            ListClustersRequest request = new ListClustersRequest().withClusterStates(ClusterState.RUNNING,
                    ClusterState.WAITING);
            return emr.listClusters(request);
        });
        for (ClusterSummary summary : clustersResult.getClusters()) {
            if (summary.getName().endsWith(clusterName)) {
                return summary.getId();
            }
        }
        return null;
    }

}
