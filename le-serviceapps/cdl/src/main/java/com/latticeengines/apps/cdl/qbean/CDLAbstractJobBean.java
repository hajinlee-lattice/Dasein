package com.latticeengines.apps.cdl.qbean;

import java.util.concurrent.Callable;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.apps.cdl.service.CDLJobService;
import com.latticeengines.apps.cdl.service.CampaignLaunchTriggerService;
import com.latticeengines.apps.cdl.service.DataFeedExecutionCleanupService;
import com.latticeengines.apps.cdl.service.DeltaCalculationService;
import com.latticeengines.apps.cdl.service.RedShiftCleanupService;
import com.latticeengines.apps.cdl.service.S3ImportService;
import com.latticeengines.apps.cdl.service.impl.CDLQuartzJobCallable;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobType;
import com.latticeengines.quartzclient.qbean.QuartzJobBean;

public abstract class CDLAbstractJobBean implements QuartzJobBean {

    private CDLJobType cdlJobType;

    @Autowired
    private CDLJobService cdlJobService;

    @Autowired
    private RedShiftCleanupService redShiftCleanupService;

    @Autowired
    private DataFeedExecutionCleanupService dataFeedExecutionCleanupService;

    @Inject
    private S3ImportService s3ImportService;

    @Inject
    private CampaignLaunchTriggerService campaignLaunchTriggerService;

    @Inject
    private DeltaCalculationService deltaCalculationService;

    @Override
    public Callable<Boolean> getCallable(String jobArguments) {
        CDLQuartzJobCallable.Builder builder = new CDLQuartzJobCallable.Builder();
        builder.cdlJobType(cdlJobType).cdlJobService(cdlJobService)
                .dataFeedExecutionCleanupService(dataFeedExecutionCleanupService)
                .redshiftCleanupService(redShiftCleanupService).s3ImportService(s3ImportService)
                .campaignLaunchTriggerService(campaignLaunchTriggerService)
                .deltaCalculationService(deltaCalculationService).jobArguments(jobArguments);
        return new CDLQuartzJobCallable(builder);
    }

    protected void setCDLJobType(CDLJobType cdlJobType) {
        this.cdlJobType = cdlJobType;
    }
}
