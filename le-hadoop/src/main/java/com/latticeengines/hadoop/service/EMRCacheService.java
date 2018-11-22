package com.latticeengines.hadoop.service;

public interface EMRCacheService {

    String getClusterId();

    String getClusterId(String clusterName);

    String getMasterIp();

    String getMasterIp(String clusterName);

    String getWebHdfsUrl();

}
