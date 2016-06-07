package com.latticeengines.dataplatform.mbean;

import java.util.concurrent.Callable;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.entitymanager.modeling.ModelEntityMgr;
import com.latticeengines.dataplatform.entitymanager.modeling.ThrottleConfigurationEntityMgr;
import com.latticeengines.dataplatform.exposed.entitymanager.JobEntityMgr;
import com.latticeengines.dataplatform.exposed.service.YarnService;
import com.latticeengines.dataplatform.service.impl.JobWatchdogCallable;
import com.latticeengines.dataplatform.service.modeling.ModelingJobService;
import com.latticeengines.quartzclient.mbean.QuartzJobBean;

@Component("jobWatchdog")
public class JobWatchdogBean implements QuartzJobBean {

    @Autowired
    private ModelingJobService modelingJobService;

    @Autowired
    private ThrottleConfigurationEntityMgr throttleConfigurationEntityMgr;

    @Autowired
    private ModelEntityMgr modelEntityMgr;

    @Autowired
    private YarnService yarnService;

    @Autowired
    private JobEntityMgr jobEntityMgr;

    @Value("${dataplatform.retry.wait.time}")
    private int retryWaitTime;

    @Override
    public Callable<Boolean> getCallable() {
        JobWatchdogCallable.Builder builder = new JobWatchdogCallable.Builder();
        builder.modelEntityMgr(modelEntityMgr)
                .modelingJobService(modelingJobService)
                .throttleConfigurationEntityMgr(throttleConfigurationEntityMgr)
                .yarnService(yarnService)
                .jobEntityMgr(jobEntityMgr)
                .retryWaitTime(retryWaitTime);

        return new JobWatchdogCallable(builder);
    }

}
