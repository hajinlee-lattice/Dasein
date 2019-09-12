package com.latticeengines.apps.cdl.util;

import java.util.Date;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.DataFeedExecutionEntityMgr;
import com.latticeengines.apps.cdl.service.DataFeedService;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecutionJobType;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("paValidationUtils")
public class PAValidationUtils {
    private static final Logger log = LoggerFactory.getLogger(PAValidationUtils.class);

    @Value("${cdl.processAnalyze.retry.expired.time}")
    private long retryExpiredTime;

    @Value("${cdl.processAnalyze.actions.import.count}")
    private int importActionCount;

    @Inject
    private DataFeedService dataFeedService;

    @Inject
    private WorkflowProxy workflowProxy;

    @Inject
    private DataFeedExecutionEntityMgr dataFeedExecutionEntityMgr;

    public void checkRetry(String customerSpace) {
        DataFeed dataFeed = dataFeedService.getOrCreateDataFeed(customerSpace);
        if (dataFeed == null) {
            String errorMessage = String. format(
                    "we can't restart processAnalyze workflow for %s, dataFeed is empty.", customerSpace);
            log.info(errorMessage);
            throw new RuntimeException(errorMessage);
        }
        DataFeedExecution execution;
        try {
            execution = dataFeedExecutionEntityMgr.findFirstByDataFeedAndJobTypeOrderByPidDesc(dataFeed,
                    DataFeedExecutionJobType.PA);
        } catch (Exception e) {
            execution = null;
        }
        if (execution == null) {
            String errorMessage = String.format("we can't restart processAnalyze workflow for %s, dataFeedExecution " +
                            "is empty."
                    , customerSpace);
            log.info(errorMessage);
            throw new RuntimeException(errorMessage);
        }
        if (!DataFeedExecution.Status.Failed.equals(execution.getStatus())) {
            String errorMessage = String.format("we can't restart processAnalyze workflow for %s, last PA isn't fail. "
                    , customerSpace);
            log.info(errorMessage);
            throw new RuntimeException(errorMessage);
        }
        long currentTime = new Date().getTime();
        if (execution.getUpdated() == null || (execution.getUpdated().getTime() - (currentTime - retryExpiredTime * 1000) < 0)) {
            String errorMessage = String.format("we can't restart processAnalyze workflow for %s, last PA has been " +
                            "more than %d second. "
                    , customerSpace, retryExpiredTime);
            log.info(errorMessage);
            throw new RuntimeException(errorMessage);
        }
    }

    private void checkDataFeedStatus(String customerSpace, DataFeed dataFeed) {
        if (dataFeed.getStatus().getDisallowedJobTypes().contains(DataFeedExecutionJobType.PA)
                && (DataFeed.Status.Initing.equals(dataFeed.getStatus()) || DataFeed.Status.Initialized.equals(dataFeed.getStatus()))) {
            String errorMessage = String.format(
                    "We can't start processAnalyze workflow for %s, need to import data first.", customerSpace);
            throw new IllegalStateException(errorMessage);
        }
    }

    public void checkStartPAValidations(String customerSpace, DataFeed dataFeed) {
        checkDataFeedStatus(customerSpace, dataFeed);
    }

}
