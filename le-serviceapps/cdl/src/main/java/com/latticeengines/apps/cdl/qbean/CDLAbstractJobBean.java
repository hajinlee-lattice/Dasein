package com.latticeengines.apps.cdl.qbean;

import java.util.concurrent.Callable;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.apps.cdl.service.CDLJobService;
import com.latticeengines.apps.cdl.service.CampaignLaunchSchedulingService;
import com.latticeengines.apps.cdl.service.DataFeedExecutionCleanupService;
import com.latticeengines.apps.cdl.service.EntityStateCorrectionService;
import com.latticeengines.apps.cdl.service.MockBrokerJobService;
import com.latticeengines.apps.cdl.service.RedShiftCleanupService;
import com.latticeengines.apps.cdl.service.S3ImportService;
import com.latticeengines.apps.cdl.service.impl.CDLQuartzJobCallable;
import com.latticeengines.apps.cdl.tray.service.TrayTestTimeoutService;
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
    private EntityStateCorrectionService entityStateCorrectionService;

    @Inject
    private CampaignLaunchSchedulingService campaignLaunchSchedulingService;

    @Inject
    private TrayTestTimeoutService trayTestTimeoutService;

    @Inject
    private MockBrokerJobService mockBrokerInstanceJobService;

    @Override
    public Callable<Boolean> getCallable(String jobArguments) {
        CDLQuartzJobCallable.Builder builder = new CDLQuartzJobCallable.Builder();
        builder.cdlJobType(cdlJobType).cdlJobService(cdlJobService)
                .dataFeedExecutionCleanupService(dataFeedExecutionCleanupService)
                .redshiftCleanupService(redShiftCleanupService).s3ImportService(s3ImportService)
                .entityStateCorrectionService(entityStateCorrectionService)
                .campaignLaunchSchedulingService(campaignLaunchSchedulingService).jobArguments(jobArguments)
                .trayTestTimeoutService(trayTestTimeoutService).mockBrokerInstanceJobService(mockBrokerInstanceJobService);
        return new CDLQuartzJobCallable(builder);
    }

    protected void setCDLJobType(CDLJobType cdlJobType) {
        this.cdlJobType = cdlJobType;
    }
}
