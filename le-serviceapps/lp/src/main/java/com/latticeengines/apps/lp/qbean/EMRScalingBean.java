package com.latticeengines.apps.lp.qbean;

import java.util.concurrent.Callable;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.quartzclient.qbean.QuartzJobBean;

@Component("emrScalingBean")
public class EMRScalingBean implements QuartzJobBean {

    @Value("${lp.emr.scaling.clusters}")
    private String scalingClusters;

    @Override
    public Callable<Boolean> getCallable(String jobArguments) {
        return EMRScalingCallable.builder() //
                .scalingClusters(scalingClusters) //
                .build();
    }

}
