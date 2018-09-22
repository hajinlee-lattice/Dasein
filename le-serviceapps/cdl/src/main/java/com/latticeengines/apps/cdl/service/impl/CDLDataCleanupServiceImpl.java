package com.latticeengines.apps.cdl.service.impl;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.CDLDataCleanupService;
import com.latticeengines.apps.cdl.workflow.CDLOperationWorkflowSubmitter;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CleanupByDateRangeConfiguration;
import com.latticeengines.domain.exposed.cdl.CleanupOperationConfiguration;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component("cdlDataCleanupService")
public class CDLDataCleanupServiceImpl implements CDLDataCleanupService {

    private static final Logger log = LoggerFactory.getLogger(CDLDataCleanupServiceImpl.class);

    private final CDLOperationWorkflowSubmitter cdlOperationWorkflowSubmitter;

    @Inject
    public CDLDataCleanupServiceImpl(CDLOperationWorkflowSubmitter cdlOperationWorkflowSubmitter) {
        this.cdlOperationWorkflowSubmitter = cdlOperationWorkflowSubmitter;
    }

    @Override
    public ApplicationId cleanupData(String customerSpace, CleanupOperationConfiguration configuration) {
        log.info("customerSpace: " + customerSpace + ", CleanupOperationConfiguration: " + configuration);
        if (configuration instanceof CleanupByDateRangeConfiguration) {
            verifyCleanupByDataRangeConfiguration((CleanupByDateRangeConfiguration) configuration);
        }
        return cdlOperationWorkflowSubmitter.submit(CustomerSpace.parse(customerSpace), configuration,
                new WorkflowPidWrapper(-1L));
    }

    private void verifyCleanupByDataRangeConfiguration(
            CleanupByDateRangeConfiguration cleanupByDateRangeConfiguration) {
        if (cleanupByDateRangeConfiguration.getStartTime() == null
                || cleanupByDateRangeConfiguration.getEndTime() == null) {
            throw new LedpException(LedpCode.LEDP_40002);
        }

        if (cleanupByDateRangeConfiguration.getStartTime().getTime() > cleanupByDateRangeConfiguration.getEndTime()
                .getTime()) {
            throw new LedpException(LedpCode.LEDP_40003);
        }
    }
}
