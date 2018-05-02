package com.latticeengines.workflowapi.steps.core;

import java.util.Map;

import javax.annotation.Resource;

import org.springframework.jdbc.core.JdbcTemplate;
import org.testng.Assert;

import com.latticeengines.workflowapi.flows.testflows.redshift.TestRedshiftWorkflowConfiguration;

public class ExportToRedshiftDeploymentTestNG extends BaseExportDeploymentTestNG<TestRedshiftWorkflowConfiguration> {

    @Resource(name = "redshiftJdbcTemplate")
    private JdbcTemplate redshiftJdbcTemplate;

    @Override
    protected TestRedshiftWorkflowConfiguration generateCreateConfiguration() {
        TestRedshiftWorkflowConfiguration.Builder builder = new TestRedshiftWorkflowConfiguration.Builder();
        return builder //
                .internalResourceHostPort(internalResourceHostPort) //
                .microServiceHostPort(microServiceHostPort) //
                .customer(mainTestCustomerSpace) //
                .updateMode(false) //
                .build();
    }

    @Override
    protected  TestRedshiftWorkflowConfiguration generateUpdateConfiguration() {
        TestRedshiftWorkflowConfiguration.Builder builder = new TestRedshiftWorkflowConfiguration.Builder();
        return builder //
                .internalResourceHostPort(internalResourceHostPort) //
                .microServiceHostPort(microServiceHostPort) //
                .customer(mainTestCustomerSpace) //
                .updateMode(true) //
                .build();
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
        Map<String, Object> row = redshiftJdbcTemplate.queryForMap(sql);
        Assert.assertTrue(row.containsKey("accountname"));
        Assert.assertTrue(row.get("accountname") instanceof String);
        String accountName = (String) row.get("accountname");
        Assert.assertTrue(accountName.startsWith(prefix));
    }

}
