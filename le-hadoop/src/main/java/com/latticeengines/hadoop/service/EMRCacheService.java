package com.latticeengines.hadoop.service;

public interface EMRCacheService {

    String getMasterIp(String clusterName);

    Boolean isEncrypted(String clusterName);

}
