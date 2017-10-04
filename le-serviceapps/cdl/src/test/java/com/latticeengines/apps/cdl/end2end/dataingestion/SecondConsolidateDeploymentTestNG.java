package com.latticeengines.apps.cdl.end2end.dataingestion;

import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.ACCOUNT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.ACCOUNT_IMPORT_SIZE_2;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.CONTACT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.CONTACT_IMPORT_SIZE_2;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class SecondConsolidateDeploymentTestNG extends DataIngestionEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(SecondConsolidateDeploymentTestNG.class);

    private StatisticsContainer preConsolidateStats;

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        resumeCheckpoint("profile1");
        verifyFirstProfileCheckpoint();

        log.info("Exporting checkpoint data to redshift. This may take more than 20 min ...");
        exportEntityToRedshift(BusinessEntity.Account);
        exportEntityToRedshift(BusinessEntity.Contact);
        exportEntityToRedshift(BusinessEntity.Product);
        exportEntityToRedshift(BusinessEntity.Transaction);
        Assert.assertEquals(countInRedshift(BusinessEntity.Account), ACCOUNT_IMPORT_SIZE_1);

        preConsolidateStats = dataCollectionProxy.getStats(mainTestTenant.getId());

        importData();
        consolidate();
        verifyConsolidate();

        verifySecondConsolidateCheckpoint();
        saveCheckpoint("consolidate2");
    }

    private void importData() throws Exception {
        mockVdbImport(BusinessEntity.Account, ACCOUNT_IMPORT_SIZE_1, ACCOUNT_IMPORT_SIZE_2);
        mockVdbImport(BusinessEntity.Contact, CONTACT_IMPORT_SIZE_1, CONTACT_IMPORT_SIZE_2);
        Thread.sleep(2000);
    }

    private void verifyConsolidate() {
        Map<TableRoleInCollection, Long> expectedCounts = ImmutableMap.of(
                BusinessEntity.Account.getServingStore(), (long) ACCOUNT_IMPORT_SIZE_2,
                BusinessEntity.Contact.getServingStore(), (long) CONTACT_IMPORT_SIZE_2);
        verifyConsolidateReport(consolidateAppId, expectedCounts);
        verifyDataFeedStatsu(DataFeed.Status.Active);
        verifyActiveVersion(initialVersion);

        StatisticsContainer postConsolidateStats = dataCollectionProxy.getStats(mainTestTenant.getId());
        Assert.assertEquals(preConsolidateStats.getName(), postConsolidateStats.getName());

        long numAccounts = ACCOUNT_IMPORT_SIZE_1 + ACCOUNT_IMPORT_SIZE_2;
        long numContacts = CONTACT_IMPORT_SIZE_1 + CONTACT_IMPORT_SIZE_2;
        Assert.assertEquals(countInRedshift(BusinessEntity.Account), numAccounts);
        Assert.assertEquals(countInRedshift(BusinessEntity.Contact), numContacts);
    }

}
