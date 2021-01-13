package com.latticeengines.apps.cdl.service.impl;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.apps.cdl.service.CDLJobService;
import com.latticeengines.apps.cdl.service.CampaignLaunchSchedulingService;
import com.latticeengines.apps.cdl.service.DataFeedExecutionCleanupService;
import com.latticeengines.apps.cdl.service.EntityStateCorrectionService;
import com.latticeengines.apps.cdl.service.MockBrokerJobService;
import com.latticeengines.apps.cdl.service.RedShiftCleanupService;
import com.latticeengines.apps.cdl.service.S3ImportService;
import com.latticeengines.apps.cdl.tray.service.TrayTestTimeoutService;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobType;

public class CDLQuartzJobCallable implements Callable<Boolean> {

    private static final Logger log = LoggerFactory.getLogger(CDLQuartzJobCallable.class);

    private CDLJobType cdlJobType;
    private CDLJobService cdlJobService;
    private DataFeedExecutionCleanupService dataFeedExecutionCleanupService;
    private RedShiftCleanupService redShiftCleanupService;
    private S3ImportService s3ImportService;
    private EntityStateCorrectionService entityStateCorrectionService;
    private CampaignLaunchSchedulingService campaignLaunchSchedulingService;
    private TrayTestTimeoutService trayTestTimeoutService;
    private MockBrokerJobService mockBrokerInstanceJobService;
    private String jobArguments;

    public CDLQuartzJobCallable(Builder builder) {
        this.cdlJobType = builder.cdlJobType;
        this.cdlJobService = builder.cdlJobService;
        this.dataFeedExecutionCleanupService = builder.dataFeedExecutionCleanupService;
        this.redShiftCleanupService = builder.redShiftCleanupService;
        this.s3ImportService = builder.s3ImportService;
        this.entityStateCorrectionService = builder.entityStateCorrectionService;
        this.campaignLaunchSchedulingService = builder.campaignLaunchSchedulingService;
        this.trayTestTimeoutService = builder.trayTestTimeoutService;
        this.mockBrokerInstanceJobService = builder.mockBrokerInstanceJobService;
        this.jobArguments = builder.jobArguments;
    }

    @Override
    public Boolean call() {
        log.info(String.format("Calling with job type: %s", cdlJobType.name()));
        switch (cdlJobType) {
            case DFEXECUTIONCLEANUP:
                return dataFeedExecutionCleanupService.removeStuckExecution(jobArguments);
            case REDSHIFTCLEANUP:
                return redShiftCleanupService.removeTempListTables() && redShiftCleanupService.removeUnusedTables();
            case IMPORT:
                return s3ImportService.submitImportJob();
            case ENTITYSTATECORRECTION:
                return entityStateCorrectionService.execute();
            case CAMPAIGNLAUNCHSCHEDULER:
                return campaignLaunchSchedulingService.kickoffScheduledCampaigns();
            case TRAYTESTTIMEOUT:
                return trayTestTimeoutService.execute();
            case MOCK_BROKER:
                return mockBrokerInstanceJobService.generateMockFiles();
            default:
                return cdlJobService.submitJob(cdlJobType, jobArguments);
        }
    }

    public static class Builder {

        private CDLJobType cdlJobType;
        private CDLJobService cdlJobService;
        private DataFeedExecutionCleanupService dataFeedExecutionCleanupService;
        private RedShiftCleanupService redShiftCleanupService;
        private S3ImportService s3ImportService;
        private EntityStateCorrectionService entityStateCorrectionService;
        private CampaignLaunchSchedulingService campaignLaunchSchedulingService;
        private TrayTestTimeoutService trayTestTimeoutService;
        private MockBrokerJobService mockBrokerInstanceJobService;
        private String jobArguments;

        public Builder() {

        }

        public Builder cdlJobType(CDLJobType cdlJobType) {
            this.cdlJobType = cdlJobType;
            return this;
        }

        public Builder cdlJobService(CDLJobService cdlJobService) {
            this.cdlJobService = cdlJobService;
            return this;
        }

        public Builder dataFeedExecutionCleanupService(
                DataFeedExecutionCleanupService dataFeedExecutionCleanupService) {
            this.dataFeedExecutionCleanupService = dataFeedExecutionCleanupService;
            return this;
        }

        public Builder redshiftCleanupService(RedShiftCleanupService redShiftCleanupService) {
            this.redShiftCleanupService = redShiftCleanupService;
            return this;
        }

        public Builder s3ImportService(S3ImportService s3ImportService) {
            this.s3ImportService = s3ImportService;
            return this;
        }

        public Builder entityStateCorrectionService(EntityStateCorrectionService entityStateCorrectionService) {
            this.entityStateCorrectionService = entityStateCorrectionService;
            return this;
        }

        public Builder campaignLaunchSchedulingService(CampaignLaunchSchedulingService campaignLaunchSchedulingService) {
            this.campaignLaunchSchedulingService = campaignLaunchSchedulingService;
            return this;
        }

        public Builder trayTestTimeoutService(TrayTestTimeoutService trayTestTimeoutService) {
            this.trayTestTimeoutService = trayTestTimeoutService;
            return this;
        }

        public Builder jobArguments(String jobArguments) {
            this.jobArguments = jobArguments;
            return this;
        }

        public Builder mockBrokerInstanceJobService(MockBrokerJobService mockBrokerInstanceJobService) {
            this.mockBrokerInstanceJobService = mockBrokerInstanceJobService;
            return this;
        }
    }
}
