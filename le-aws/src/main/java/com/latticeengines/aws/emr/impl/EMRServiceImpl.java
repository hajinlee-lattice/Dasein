package com.latticeengines.aws.emr.impl;

import javax.annotation.Resource;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
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
import com.amazonaws.services.elasticmapreduce.model.ListClustersRequest;
import com.amazonaws.services.elasticmapreduce.model.ListClustersResult;
import com.amazonaws.services.elasticmapreduce.model.ListInstancesRequest;
import com.amazonaws.services.elasticmapreduce.model.ListInstancesResult;
import com.latticeengines.aws.emr.EMRService;

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
        ListClustersRequest request = new ListClustersRequest().withClusterStates(ClusterState.RUNNING,
                ClusterState.WAITING);
        ListClustersResult clustersResult = emr.listClusters(request);
        String masterIp = null;
        for (ClusterSummary summary : clustersResult.getClusters()) {
            if (summary.getName().endsWith(clusterName)) {
                log.info("Found an EMR cluster named " + summary.getName());
                String clusterId = summary.getId();
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
        }
        return masterIp;
    }

    @Override
    public String getWebHdfsUrl() {
        return "http://" + getMasterIp() + ":50070/webhdfs/v1";
    }

    private AmazonElasticMapReduce getEmr() {
        if (emrClient == null) {
            synchronized (this) {
                if (emrClient == null) {
                    emrClient = AmazonElasticMapReduceClientBuilder.standard() //
                            .withCredentials(new AWSStaticCredentialsProvider(awsCredentials)) //
                            .withRegion(Regions.fromName(region)) //
                            .build();

                    ListClustersRequest request = new ListClustersRequest().withClusterStates(ClusterState.RUNNING,
                            ClusterState.WAITING);
                    ListClustersResult result = emrClient.listClusters(request);
                    log.info("There are " + CollectionUtils.size(result.getClusters()) + " clusters.");

                    log.info("Generating an emr client using creds: " + awsCredentials.getAWSAccessKeyId());
                }
            }
        }
        return emrClient;
    }

}
