package com.latticeengines.apps.cdl.service.impl;

import java.util.Date;
import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.CDLDataCleanupService;
import com.latticeengines.apps.cdl.workflow.CDLOperationWorkflowSubmitter;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CleanupAllConfiguration;
import com.latticeengines.domain.exposed.cdl.CleanupByDateRangeConfiguration;
import com.latticeengines.domain.exposed.cdl.CleanupByUploadConfiguration;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.cdl.MaintenanceOperationType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component("cdlDataCleanupService")
public class CDLDataCleanupServiceImpl implements CDLDataCleanupService {

    private static final Logger log = LoggerFactory.getLogger(CDLDataCleanupServiceImpl.class);

    private final CDLOperationWorkflowSubmitter cdlOperationWorkflowSubmitter;

    @Inject
    public CDLDataCleanupServiceImpl(CDLOperationWorkflowSubmitter cdlOperationWorkflowSubmitter) {
        this.cdlOperationWorkflowSubmitter = cdlOperationWorkflowSubmitter;
    }

    @Override
    public ApplicationId cleanupAll(String customerSpace, BusinessEntity entity) {
        CleanupAllConfiguration cleanupAllConfiguration = new CleanupAllConfiguration();
        cleanupAllConfiguration.setOperationType(MaintenanceOperationType.DELETE);
        cleanupAllConfiguration.setCleanupOperationType(CleanupOperationType.ALL);
        cleanupAllConfiguration.setEntity(entity);
        cleanupAllConfiguration.setCustomerSpace(customerSpace);
        return cdlOperationWorkflowSubmitter.submit(CustomerSpace.parse(customerSpace), cleanupAllConfiguration);
    }

    @Override
    public ApplicationId cleanupAllData(String customerSpace, BusinessEntity entity) {
        CleanupAllConfiguration cleanupAllConfiguration = new CleanupAllConfiguration();
        cleanupAllConfiguration.setOperationType(MaintenanceOperationType.DELETE);
        cleanupAllConfiguration.setCleanupOperationType(CleanupOperationType.ALLDATA);
        cleanupAllConfiguration.setEntity(entity);
        cleanupAllConfiguration.setCustomerSpace(customerSpace);
        return cdlOperationWorkflowSubmitter.submit(CustomerSpace.parse(customerSpace), cleanupAllConfiguration);
    }

    @Override
    public ApplicationId cleanupByTimeRange(String customerSpace, BusinessEntity entity, Date startTime, Date endTime) {
        if(startTime == null || endTime == null) {
            throw new LedpException(LedpCode.LEDP_40002);
        }

        if(startTime.getTime() > endTime.getTime()) {
            throw new LedpException(LedpCode.LEDP_40003);
        }

        CleanupByDateRangeConfiguration cleanupByDateRangeConfiguration = new CleanupByDateRangeConfiguration();
        cleanupByDateRangeConfiguration.setOperationType(MaintenanceOperationType.DELETE);
        cleanupByDateRangeConfiguration.setCleanupOperationType(CleanupOperationType.BYDATERANGE);
        cleanupByDateRangeConfiguration.setStartTime(startTime);
        cleanupByDateRangeConfiguration.setEndTime(endTime);
        cleanupByDateRangeConfiguration.setEntity(entity);
        cleanupByDateRangeConfiguration.setCustomerSpace(customerSpace);
        return cdlOperationWorkflowSubmitter.submit(CustomerSpace.parse(customerSpace), cleanupByDateRangeConfiguration);
    }

    @Override
    public ApplicationId cleanupByUpload(String customerSpace, CleanupByUploadConfiguration configuration) {
        configuration.setCustomerSpace(customerSpace);
        log.info("customerSpace: " + customerSpace +
                ", configuration.getCleanupOperationType(): " + configuration.getCleanupOperationType() +
                ", configuration.getFilePath(): " + configuration.getFilePath() +
                ", configuration.getTableName(): " + configuration.getTableName() +
                ", configuration.getEntity(): " + configuration.getEntity());
        return cdlOperationWorkflowSubmitter.submit(CustomerSpace.parse(customerSpace), configuration);
    }
}
