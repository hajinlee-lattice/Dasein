package com.latticeengines.workflowapi.steps.core;

import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportToRedshiftStepConfiguration;
import com.latticeengines.redshiftdb.exposed.service.RedshiftPartitionService;
import com.latticeengines.workflowapi.flows.testflows.framework.TestFrameworkWrapperWorkflowConfiguration;
import com.latticeengines.workflowapi.flows.testflows.framework.sampletests.SamplePostprocessingStepConfiguration;
import com.latticeengines.workflowapi.flows.testflows.redshift.PrepareTestRedshiftConfiguration;

public class ExportToRedshiftDeploymentTestNG extends
        BaseExportDeploymentTestNG<TestFrameworkWrapperWorkflowConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ExportToRedshiftDeploymentTestNG.class);

    @Inject
    private RedshiftPartitionService redshiftPartitionService;

    @Override
    protected TestFrameworkWrapperWorkflowConfiguration generateCreateConfiguration() {
        return generateConfiguration(false);
    }

    @Override
    protected TestFrameworkWrapperWorkflowConfiguration generateUpdateConfiguration() {
        return generateConfiguration(true);
    }

    private TestFrameworkWrapperWorkflowConfiguration generateConfiguration(boolean isUpdateMode) {
        PrepareTestRedshiftConfiguration prepareTestRedshiftConfig = new PrepareTestRedshiftConfiguration(
                "prepareTestRedshift");
        prepareTestRedshiftConfig.setUpdateMode(isUpdateMode);
        SamplePostprocessingStepConfiguration postStepConfig = new SamplePostprocessingStepConfiguration(
                "samplePostprocessingStep");

        ExportToRedshiftStepConfiguration exportToRedshiftStepConfig = new ExportToRedshiftStepConfiguration();
        exportToRedshiftStepConfig.setCustomerSpace(mainTestCustomerSpace);

        return generateStepTestConfiguration(prepareTestRedshiftConfig, "exportToRedshift",
                exportToRedshiftStepConfig, postStepConfig);
    }


    @Override
    protected  void verifyCreateExport() {
        verifyAccountNamePrefix("ACC0001", "ACC1");
        verifyAccountNamePrefix("ACC0007", "ACC1");
    }

    @Override
    protected  void verifyUpdateExport() {
        verifyAccountNamePrefix("ACC0001", "ACC2");
        verifyAccountNamePrefix("ACC0007", "ACC1");
    }

    private void verifyAccountNamePrefix(String accountId, String prefix) {
        String tenantName = mainTestCustomerSpace.getTenantId();
        String redshiftTable = tenantName + "_" + TABLE_1;
        String sql = "SELECT AccountName from %s WHERE AccountId = '%s'";
        sql = String.format(sql, redshiftTable, accountId);
        Map<String, Object> row = redshiftPartitionService.getBatchUserJdbcTemplate(null).queryForMap(sql);
        Assert.assertTrue(row.containsKey("accountname"));
        Assert.assertTrue(row.get("accountname") instanceof String);
        String accountName = (String) row.get("accountname");
        Assert.assertTrue(accountName.startsWith(prefix));
    }

}
