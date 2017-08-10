package com.latticeengines.serviceapps.cdl.end2end.dataingestion;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.CEAttr;
import static org.testng.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.Buckets;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.JobStatus;

public class FirstConsolidateAndProfileDeploymentTestNG extends DataIngestionEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(FirstConsolidateAndProfileDeploymentTestNG.class);

    @Test(groups = "end2end")
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
        DataFeed dataFeed = dataFeedProxy.getDataFeed(mainTestTenant.getId());
        Assert.assertEquals(DataFeed.Status.InitialConsolidated, dataFeed.getStatus());

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
        DataFeed dataFeed = dataFeedProxy.getDataFeed(mainTestTenant.getId());
        Assert.assertEquals(DataFeed.Status.Active, dataFeed.getStatus());

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

        Statistics statistics = statisticsContainer.getStatistics();
        verifyStatistics(statistics);

        Table bucketedContactTable = dataCollectionProxy.getTable(customerSpace,
                BusinessEntity.Contact.getServingStore());
        Assert.assertNotNull(bucketedContactTable);
        attributes = bucketedContactTable.getAttributes();
        for (Attribute attribute : attributes) {
            Assert.assertFalse(attribute.getName().contains(CEAttr),
                    "Should not have encoded attr " + attribute.getName() + " in expanded table.");
        }
    }

    private void verifyStatistics(Statistics statistics) {
        statistics.getCategories().values().forEach(catStats -> //
        catStats.getSubcategories().values().forEach(subCatStats -> {
            subCatStats.getAttributes().forEach(this::verifyAttrStats);
        }));
    }

    private void verifyAttrStats(AttributeLookup lookup, AttributeStats attributeStats) {
        Assert.assertNotNull(attributeStats.getNonNullCount());
        Assert.assertTrue(attributeStats.getNonNullCount() >= 0);
        Buckets buckets = attributeStats.getBuckets();
        if (buckets != null) {
            Assert.assertNotNull(buckets.getType());
            Assert.assertFalse(buckets.getBucketList() == null || buckets.getBucketList().isEmpty(),
                    "Bucket list for " + lookup + " is empty.");
            Long sum = buckets.getBucketList().stream().mapToLong(Bucket::getCount).sum();
            Assert.assertEquals(sum, attributeStats.getNonNullCount());
        }
    }

}
