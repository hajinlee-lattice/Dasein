package com.latticeengines.apps.cdl.end2end;


import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigUpdateMode;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.CDLAttrConfigProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;


public class AccountAttributeQuotaLimitDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final String jsonFileName = "cg-tenant-registration-attribute-limit.json";

    private static final Logger log = LoggerFactory.getLogger(AccountAttributeQuotaLimitDeploymentTestNG.class);

    @Inject
    private WorkflowProxy workflowProxy;

    @Inject
    private CDLAttrConfigProxy cdlAttrConfigProxy;

    @BeforeClass(groups = "end2end")
    public void setup() {
        setupEnd2EndTestEnvironmentByFile(jsonFileName);
    }

    @Test(groups = "end2end")
    public void testDataQuotaLimit() throws Exception {
        resumeCheckpoint(ProcessAccountDeploymentTestNG.CHECK_POINT);
        testCurrentAttributeNum();
        importData();
        processAnalyze(JobStatus.FAILED);
        setInactiveAttr();
        processAnalyze(JobStatus.COMPLETED);
    }

    //account limit is 47, the table with role ConsolidatedAccount has 48 user fields, inactivate 2 attrs,
    // pa should pass as the check 48 -2 < 47
    private void setInactiveAttr() {
        AttrConfig config1 = new AttrConfig();
        config1.setAttrName("Industry");
        config1.setEntity(BusinessEntity.Account);
        AttrConfig config2 = new AttrConfig();
        config2.setAttrName("Website");
        config2.setEntity(BusinessEntity.Account);
        AttrConfigProp<AttrState> prop = new AttrConfigProp<>();
        prop.setCustomValue(AttrState.Inactive);
        config1.setAttrProps(ImmutableMap.of(ColumnMetadataKey.State, prop));
        config2.setAttrProps(ImmutableMap.of(ColumnMetadataKey.State, prop));
        AttrConfigRequest request = new AttrConfigRequest();
        request.setAttrConfigs(Arrays.asList(config1, config2));
        cdlAttrConfigProxy.saveAttrConfig(mainCustomerSpace, request, AttrConfigUpdateMode.Limit);
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
        importData(BusinessEntity.Account, "Account_different_schema.csv");
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.InitialLoaded.getName());

    }

    protected void processAnalyze(JobStatus result) {
        log.info("Start processing and analyzing ...");
        ApplicationId appId = cdlProxy.processAnalyze(mainTestTenant.getId(), null);
        processAnalyzeAppId = appId.toString();
        log.info("processAnalyzeAppId=" + processAnalyzeAppId);
        JobStatus status = waitForWorkflowStatus(appId.toString(), false);
        Assert.assertEquals(status, result);
        if (JobStatus.FAILED.equals(result)) {
            Job job = workflowProxy.getWorkflowJobFromApplicationId(appId.toString(),
                    CustomerSpace.parse(mainTestTenant.getId()).toString());
            Assert.assertEquals(job.getErrorMsg(), "The input contains more than 47 fields. Please reduce the no. of " +
                    "Account fields and try again");
        }
    }

}
