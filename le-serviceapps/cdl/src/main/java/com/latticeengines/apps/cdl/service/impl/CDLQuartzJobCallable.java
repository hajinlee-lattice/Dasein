package com.latticeengines.apps.cdl.service.impl;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.apps.cdl.service.CDLJobService;
import com.latticeengines.apps.cdl.service.DataFeedExecutionCleanupService;
import com.latticeengines.apps.cdl.service.RedShiftCleanupService;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobType;

public class CDLQuartzJobCallable implements Callable<Boolean> {

    private static final Logger log = LoggerFactory.getLogger(CDLQuartzJobCallable.class);

    private CDLJobType cdlJobType;
    private CDLJobService cdlJobService;
    private DataFeedExecutionCleanupService dataFeedExecutionCleanupService;
    private RedShiftCleanupService redShiftCleanupService;
    private String jobArguments;

    public CDLQuartzJobCallable(Builder builder) {
        this.cdlJobType = builder.cdlJobType;
        this.cdlJobService = builder.cdlJobService;
        this.dataFeedExecutionCleanupService = builder.dataFeedExecutionCleanupService;
        this.redShiftCleanupService = builder.redShiftCleanupService;
        this.jobArguments = builder.jobArguments;
    }

    @Override
    public Boolean call() throws Exception {
        log.info(String.format("Calling with job type: %s", cdlJobType.name()));
        if (CDLJobType.DFEXECUTIONCLEANUP.equals(cdlJobType)) {
            return dataFeedExecutionCleanupService.removeStuckExecution(jobArguments);
        } else if (CDLJobType.REDSHIFTCLEANUP.equals(cdlJobType)){
            return redShiftCleanupService.removeUnusedTables();
        } else {
            return cdlJobService.submitJob(cdlJobType, jobArguments);
        }
    }

    public static class Builder {

        private CDLJobType cdlJobType;
        private CDLJobService cdlJobService;
        private DataFeedExecutionCleanupService dataFeedExecutionCleanupService;
        private RedShiftCleanupService redShiftCleanupService;
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

        public Builder dataFeedExecutionCleanupService(DataFeedExecutionCleanupService dataFeedExecutionCleanupService) {
            this.dataFeedExecutionCleanupService = dataFeedExecutionCleanupService;
            return this;
        }

        public Builder redshiftCleanupService(RedShiftCleanupService redShiftCleanupService) {
            this.redShiftCleanupService = redShiftCleanupService;
            return this;
        }

        public Builder jobArguments(String jobArguments) {
            this.jobArguments = jobArguments;
            return this;
        }
    }
}
