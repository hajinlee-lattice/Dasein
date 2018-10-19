package com.latticeengines.yarn.exposed.service.impl;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.latticeengines.yarn.exposed.service.EMREnvService;

@Service("emrEnvService")
public class EMREnvServiceImpl implements EMREnvService {

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
}
