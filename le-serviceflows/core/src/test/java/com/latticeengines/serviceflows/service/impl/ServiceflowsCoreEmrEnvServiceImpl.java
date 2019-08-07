package com.latticeengines.serviceflows.service.impl;

import java.util.List;

import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.latticeengines.domain.exposed.yarn.ApplicationMetrics;
import com.latticeengines.domain.exposed.yarn.ClusterMetrics;
import com.latticeengines.yarn.exposed.service.EMREnvService;


@Service("serviceflowsCoreEmrEnvServiceImpl")
public class ServiceflowsCoreEmrEnvServiceImpl implements EMREnvService {

    @Value("${dataplatform.python.conda.env}")
    private String condaEnv;

    @Value("${dataplatform.python.conda.env.ambari}")
    private String condaEnvAmbari;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Value("${dataplatform.queue.scheme}")
    private String ambariQueueScheme;

    @Override
    public String getLatticeCondaEnv() {
        if (Boolean.TRUE.equals(useEmr)) {
            return condaEnv;
        } else {
            return condaEnvAmbari;
        }
    }

    @Override
    public String getYarnQueueScheme() {
        if (Boolean.TRUE.equals(useEmr)) {
            return "default";
        } else {
            return ambariQueueScheme;
        }
    }

    @Override
    public YarnConfiguration getYarnConfiguration(String emrCluster) {
        return null;
    }

    @Override
    public YarnClient getYarnClient(String emrCluster) {
        return null;
    }

    @Override
    public ClusterMetrics getClusterMetrics(String emrCluster) {
        return null;
    }

    @Override
    public List<ApplicationMetrics> getAppMetrics(String clusterId, YarnApplicationState... states) {
        return null;
    }

}
