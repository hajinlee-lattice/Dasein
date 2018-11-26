package com.latticeengines.aws.emr;

import com.amazonaws.services.elasticmapreduce.model.InstanceGroup;

public interface EMRService {

    String getClusterId(String clusterName);

    String getMasterIp(String clusterId);

    boolean isActive(String clusterId);

    boolean isEncrypted(String clusterId);

    String getLogBucket(String clusterId);

    String getSqoopHostPort();

    InstanceGroup getTaskGroup(String clusterId);

    InstanceGroup getCoreGroup(String clusterId);

    void scaleTaskGroup(String clusterId, int targetCount);

    void scaleTaskGroup(InstanceGroup taskGrp, int targetCount);

}
