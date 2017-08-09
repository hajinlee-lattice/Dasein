package com.latticeengines.serviceapps.cdl.end2end.dataingestion;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.CEAttr;
import static org.testng.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.JobStatus;

public class FirstConsolidateAndProfileDeploymentTestNG extends DataIngestionEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(FirstConsolidateAndProfileDeploymentTestNG.class);

    @Test(groups = "deployment")
    public void testFirstConsolidate() throws Exception {
        importData();
        consolidate();
        verifyConsolidate();
        profile();
        verifyProfile();
    }

    private void importData() throws Exception {
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());
        mockAvroData(0, 300);
        Thread.sleep(2000);
        mockAvroData(300, 200);
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.InitialLoaded.getName());
    }

    private void consolidate() {
        log.info("Start consolidating ...");
        ApplicationId appId = cdlProxy.consolidate(mainTestTenant.getId());
        JobStatus completedStatus = waitForWorkflowStatus(appId.toString(), false);
        assertEquals(completedStatus, JobStatus.COMPLETED);
    }

    private void verifyConsolidate() {
        long numAccounts = countTableRole(BusinessEntity.Account.getBatchStore());
        Assert.assertEquals(numAccounts, 300);
        long numContacts = countTableRole(BusinessEntity.Contact.getBatchStore());
        Assert.assertEquals(numContacts, 300);
    }

    private void profile() throws IOException {
        log.info("Start profiling ...");
        ApplicationId appId = cdlProxy.profile(mainTestTenant.getId());
        JobStatus completedStatus = waitForWorkflowStatus(appId.toString(), false);
        assertEquals(completedStatus, JobStatus.COMPLETED);
    }

    private void verifyProfile() throws IOException {
        String customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        Table bucketedAccountTable = dataCollectionProxy.getTable(customerSpace,
                BusinessEntity.Account.getServingStore());
        Assert.assertNotNull(bucketedAccountTable);
        List<Attribute> attributes = bucketedAccountTable.getAttributes();
        for (Attribute attribute : attributes) {
            Assert.assertFalse(attribute.getName().contains(CEAttr),
                    "Should not have encoded attr " + attribute.getName() + " in expanded table.");
        }
        StatisticsContainer statisticsContainer = dataCollectionProxy.getStats(customerSpace);
        Assert.assertNotNull(statisticsContainer);
        // save stats to a local json to help create verifications
        File statsJson = new File("stats.json");
        FileUtils.deleteQuietly(statsJson);
        FileUtils.write(statsJson, JsonUtils.pprint(statisticsContainer));

        Table bucketedContactTable = dataCollectionProxy.getTable(customerSpace,
                BusinessEntity.Contact.getServingStore());
        Assert.assertNotNull(bucketedContactTable);
        attributes = bucketedContactTable.getAttributes();
        for (Attribute attribute : attributes) {
            Assert.assertFalse(attribute.getName().contains(CEAttr),
                    "Should not have encoded attr " + attribute.getName() + " in expanded table.");
        }
    }

}
