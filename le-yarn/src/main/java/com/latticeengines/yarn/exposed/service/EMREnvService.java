package com.latticeengines.yarn.exposed.service;

import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.latticeengines.domain.exposed.yarn.ClusterMetrics;

public interface EMREnvService {

    String getLatticeCondaEnv();

    String getYarnQueueScheme();

    YarnConfiguration getYarnConfiguration(String emrCluster);

    org.springframework.yarn.client.YarnClient getSpringYarnClient(String emrCluster);

    YarnClient getYarnClient(String emrCluster);

    ClusterMetrics getClusterMetrics(String emrCluster);

}
