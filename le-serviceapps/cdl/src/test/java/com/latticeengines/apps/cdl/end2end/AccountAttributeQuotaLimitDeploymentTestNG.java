package com.latticeengines.apps.cdl.end2end;


import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;


public class AccountAttributeQuotaLimitDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final String jsonFileName = "cg-tenant-registration-attribute-limit.json";

    private static final Logger log = LoggerFactory.getLogger(AccountAttributeQuotaLimitDeploymentTestNG.class);

    @Inject
    private WorkflowProxy workflowProxy;

    @BeforeClass(groups = "end2end")
    public void setup() {
        setupEnd2EndTestEnvironmentByFile(jsonFileName);
    }

    @Test(groups = "end2end")
    public void testDataQuotaLimit() throws Exception {
        resumeCheckpoint(ProcessAccountDeploymentTestNG.CHECK_POINT);
        testCurrentAttributeNum();
        importData();
        processAnalyze();
    }

    private void testCurrentAttributeNum() {
        Table table = dataCollectionProxy.getTable(mainCustomerSpace, BusinessEntity.Account.getBatchStore());
        List<Attribute> attrs = table.getAttributes();
        // 29 attribute in batch store table,
        Assert.assertEquals(attrs.size(), 29);
    }
    private void importData() {
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());
        // 20 same attribute with account table in checking point
        mockCSVImport(BusinessEntity.Account, 1, "DefaultSystem_AccountData");
        importData(BusinessEntity.Account, "Account_different_schema.csv");
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.InitialLoaded.getName());

    }

    protected void processAnalyze() {
        log.info("Start processing and analyzing ...");
        ApplicationId appId = cdlProxy.processAnalyze(mainTestTenant.getId(), null);
        processAnalyzeAppId = appId.toString();
        log.info("processAnalyzeAppId=" + processAnalyzeAppId);
        JobStatus status = waitForWorkflowStatus(appId.toString(), false);
        Assert.assertEquals(status, JobStatus.FAILED);
        Job job = workflowProxy.getWorkflowJobFromApplicationId(appId.toString(),
                CustomerSpace.parse(mainTestTenant.getId()).toString());
        Assert.assertEquals(job.getErrorMsg(), "The input contains more than 30 fields. Please reduce the no. of Account fields and try again");
    }

}
