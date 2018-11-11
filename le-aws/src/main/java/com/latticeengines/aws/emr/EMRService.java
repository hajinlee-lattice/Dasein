package com.latticeengines.aws.emr;

import com.amazonaws.services.elasticmapreduce.model.InstanceGroup;

public interface EMRService {

    String getMasterIp();

    String getClusterId();

    String getMasterIp(String clusterName);

    String getMasterIpByClusterId(String clusterId);

    boolean isActive(String clusterId);

    boolean isEncrypted(String clusterName);

    String getClusterId(String clusterName);

    String getLogBucket(String clusterId);

    String getWebHdfsUrl();

    String getSqoopHostPort();

    InstanceGroup getTaskGroup(String clusterName);

    void scaleTaskGroup(String clusterName, int targetCount);

}
