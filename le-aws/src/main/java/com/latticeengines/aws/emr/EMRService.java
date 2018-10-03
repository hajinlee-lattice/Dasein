package com.latticeengines.aws.emr;

public interface EMRService {

    String getMasterIp();

    String getMasterIp(String clusterName);

    String getWebHdfsUrl();

    String getSqoopHostPort();

}
