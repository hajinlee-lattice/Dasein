package com.latticeengines.yarn.exposed.service;

import java.util.List;

import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.latticeengines.domain.exposed.yarn.ApplicationMetrics;
import com.latticeengines.domain.exposed.yarn.ClusterMetrics;

public interface EMREnvService {

    String getLatticeCondaEnv();

    String getYarnQueueScheme();

    YarnConfiguration getYarnConfiguration(String clusterId);

    YarnClient getYarnClient(String clusterId);

    ClusterMetrics getClusterMetrics(String clusterId);

    List<ApplicationMetrics> getAppMetrics(String clusterId, YarnApplicationState... states);

}
