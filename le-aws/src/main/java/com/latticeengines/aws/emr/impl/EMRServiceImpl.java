package com.latticeengines.aws.emr.impl;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
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

    @Inject
    private AmazonElasticMapReduceClient emrClient;

    @Value("${aws.emr.cluster}")
    private String clusterName;

    @Override
    public String getMasterIp() {
        return getMasterIp(clusterName);
    }

    @Override
    public String getMasterIp(String clusterName) {
        ListClustersRequest request = new ListClustersRequest().withClusterStates(ClusterState.RUNNING,
                ClusterState.WAITING);
        ListClustersResult clustersResult = emrClient.listClusters(request);
        String masterIp = null;
        for (ClusterSummary summary : clustersResult.getClusters()) {
            if (summary.getName().endsWith(clusterName)) {
                log.info("Found an EMR cluster named " + summary.getName());
                String clusterId = summary.getId();
                DescribeClusterResult cluster = emrClient
                        .describeCluster(new DescribeClusterRequest().withClusterId(clusterId));
                String masterDNS = cluster.getCluster().getMasterPublicDnsName();
                ListInstancesResult instances = emrClient
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

}
