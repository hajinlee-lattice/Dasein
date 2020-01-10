package com.latticeengines.apps.cdl.qbean;

import java.util.concurrent.Callable;

import org.quartz.DisallowConcurrentExecution;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobType;
import com.latticeengines.quartzclient.qbean.QuartzJobBean;

@DisallowConcurrentExecution
@Component("cdlCampaignLaunchSchedulerJob")
public class CDLCampaignLaunchSchedulerJob extends CDLAbstractJobBean implements QuartzJobBean {

    @Override
    public Callable<Boolean> getCallable(String jobArguments) {
        super.setCDLJobType(CDLJobType.CAMPAIGNLAUNCHSCHEDULER);
        return super.getCallable(jobArguments);
    }
}
