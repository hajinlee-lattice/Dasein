package com.latticeengines.cdl.workflow.listeners;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.serviceflows.cdl.ImportListSegmentWorkflowConfiguration;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("importListSegmentListener")
public class ImportListSegmentListener extends LEJobListener {

    private static final Logger log = LoggerFactory.getLogger(ImportListSegmentListener.class);

    @Inject
    private DataUnitProxy dataUnitProxy;

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {
    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        String tenantId = jobExecution.getJobParameters().getString("CustomerSpace");
        String accountDataUnitName = getStringValueFromContext(jobExecution, ImportListSegmentWorkflowConfiguration.ACCOUNT_DATA_UNIT_NAME);
        log.info(String.format("Tenant id is %s, account data unit name is %s.", tenantId, accountDataUnitName));
        dataUnitProxy.registerAthenaDataUnit(tenantId, accountDataUnitName);
        String preAthenaDataUnitName = getStringValueFromContext(jobExecution, ImportListSegmentWorkflowConfiguration.PREVIOUS_ACCOUNT_ATHENA_UNIT_NAME);
        if (StringUtils.isNotEmpty(preAthenaDataUnitName)) {
            log.info(String.format("will delete athena data unit with name %s", preAthenaDataUnitName));
            dataUnitProxy.delete(tenantId, preAthenaDataUnitName, DataUnit.StorageType.Athena);
        }
    }
}
