package com.latticeengines.apps.lp.qbean;

import java.util.concurrent.Callable;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.aws.emr.EMRService;
import com.latticeengines.hadoop.exposed.service.EMRCacheService;
import com.latticeengines.quartzclient.qbean.QuartzJobBean;
import com.latticeengines.yarn.exposed.service.EMREnvService;

@Component("emrScalingBean")
public class EMRScalingBean implements QuartzJobBean {

    @Value("${lp.emr.scaling.clusters}")
    private String scalingClusters;

    @Value("${lp.emr.scaling.min.task.nodes}")
    private int minTaskNodes;

    @Inject
    private EMRService emrService;

    @Inject
    private EMRCacheService emrCacheService;

    @Inject
    private EMREnvService emrEnvService;

    @Override
    public Callable<Boolean> getCallable(String jobArguments) {
        return EMRScalingCallable.builder() //
                .scalingClusters(scalingClusters) //
                .minTaskNodes(minTaskNodes) //
                .emrEnvService(emrEnvService) //
                .emrCacheService(emrCacheService)
                .emrService(emrService) //
                .build();
    }

}
