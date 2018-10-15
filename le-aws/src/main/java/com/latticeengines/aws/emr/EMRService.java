package com.latticeengines.aws.emr;

import com.amazonaws.services.elasticmapreduce.model.InstanceGroup;

public interface EMRService {

    String getMasterIp();

    String getMasterIp(String clusterName);

    String getWebHdfsUrl();

    String getSqoopHostPort();

    InstanceGroup getTaskGroup(String clusterName);

    void scaleTaskGroup(String clusterName, int targetCount);

}
