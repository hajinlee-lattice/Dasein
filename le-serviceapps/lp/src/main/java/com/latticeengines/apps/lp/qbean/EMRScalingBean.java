package com.latticeengines.apps.lp.qbean;

import java.util.concurrent.Callable;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.aws.emr.EMRService;
import com.latticeengines.quartzclient.qbean.QuartzJobBean;
import com.latticeengines.yarn.exposed.service.EMREnvService;

@Component("emrScalingBean")
public class EMRScalingBean implements QuartzJobBean {

    @Value("${lp.emr.scaling.clusters}")
    private String scalingClusters;

    @Inject
    private EMRService emrService;

    @Inject
    private EMREnvService emrEnvService;

    @Override
    public Callable<Boolean> getCallable(String jobArguments) {
        return EMRScalingCallable.builder() //
                .scalingClusters(scalingClusters) //
                .emrEnvService(emrEnvService) //
                .emrService(emrService) //
                .build();
    }

}
