package com.latticeengines.aws.emr;

import java.util.List;
import java.util.function.Predicate;

import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.amazonaws.services.elasticmapreduce.model.Instance;
import com.amazonaws.services.elasticmapreduce.model.InstanceFleet;
import com.amazonaws.services.elasticmapreduce.model.InstanceGroup;

public interface EMRService {

    String getClusterId(String clusterName);

    String getMasterIp(String clusterId);

    boolean isActive(String clusterId);

    String getLogBucket(String clusterId);

    String getSqoopHostPort();

    InstanceGroup getTaskGroup(String clusterId);

    InstanceGroup getCoreGroup(String clusterId);

    InstanceFleet getTaskFleet(String clusterId);

    InstanceFleet getCoreFleet(String clusterId);

    List<Instance> getRunningNodeFromTaskGroup(String clusterId, String taskGrpId);

    void scaleTaskFleet(String clusterId, InstanceFleet taskFleet, int targetOnDemandCount, int targetSpotCount);

    void scaleTaskGroup(String clusterId, InstanceGroup taskGrp, int targetCount);

    void terminateTaskInstances(String clusterId, String taskGrpId, List<String> ec2InstanceIds);

    List<ClusterSummary> findClusters(Predicate<ClusterSummary> filter);

}
