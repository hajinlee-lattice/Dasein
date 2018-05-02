package com.latticeengines.apps.cdl.end2end.dataingestion;

import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.ACCOUNT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.CONTACT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.PRODUCT_IMPORT_SIZE_1;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;

/**
 * Process Account, Contact and Product for a new tenant
 */
public class ProcessAccountDeploymentTestNG extends DataIngestionEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ProcessAccountDeploymentTestNG.class);

    static final String CHECK_POINT = "process1";

    public static final int ACCOUNT_IMPORT_SIZE_1_1 = 350;
    public static final int ACCOUNT_IMPORT_SIZE_1_2 = 150;
    private static final int CONTACT_IMPORT_SIZE_1_1 = 600;
    private static final int CONTACT_IMPORT_SIZE_1_2 = 500;
    private static final int PRODUCT_IMPORT_SIZE_1_1 = 70;
    private static final int PRODUCT_IMPORT_SIZE_1_2 = 30;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        try {
            runPreCheckin();
        } finally {
            saveCheckpoint(CHECK_POINT);
        }
    }

    @Test(groups = "precheckin")
    public void runPreCheckin() throws Exception {
        Assert.assertEquals(ACCOUNT_IMPORT_SIZE_1_1 + ACCOUNT_IMPORT_SIZE_1_2, ACCOUNT_IMPORT_SIZE_1);
        Assert.assertEquals(CONTACT_IMPORT_SIZE_1_1 + CONTACT_IMPORT_SIZE_1_2, CONTACT_IMPORT_SIZE_1);
        Assert.assertEquals(PRODUCT_IMPORT_SIZE_1_1 + PRODUCT_IMPORT_SIZE_1_2, PRODUCT_IMPORT_SIZE_1);

        importData();
        processAnalyze();
        verifyProcess();
    }

    private void importData() throws Exception {
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());
        mockVdbImport(BusinessEntity.Account, 0, ACCOUNT_IMPORT_SIZE_1_1);
        mockVdbImport(BusinessEntity.Contact, 0, CONTACT_IMPORT_SIZE_1_1);
        mockVdbImport(BusinessEntity.Product, 0, PRODUCT_IMPORT_SIZE_1_1);
        Thread.sleep(2000);
        mockVdbImport(BusinessEntity.Account, ACCOUNT_IMPORT_SIZE_1_1, ACCOUNT_IMPORT_SIZE_1_2);
        mockVdbImport(BusinessEntity.Contact, CONTACT_IMPORT_SIZE_1_1, CONTACT_IMPORT_SIZE_1_2);
        mockVdbImport(BusinessEntity.Product, PRODUCT_IMPORT_SIZE_1_1, PRODUCT_IMPORT_SIZE_1_2);
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.InitialLoaded.getName());
    }

    private void verifyProcess() {
        verifyDataFeedStatus(DataFeed.Status.Active);
        verifyActiveVersion(DataCollection.Version.Green);

        verifyProcessAnalyzeReport(processAnalyzeAppId);

        StatisticsContainer statisticsContainer = dataCollectionProxy.getStats(mainTestTenant.getId());
        Assert.assertNotNull(statisticsContainer, "Should have statistics in active version");

        verifyStats(BusinessEntity.Account, BusinessEntity.Contact);

        long numAccounts = ACCOUNT_IMPORT_SIZE_1;
        long numContacts = CONTACT_IMPORT_SIZE_1;
        long numProducts = PRODUCT_IMPORT_SIZE_1;

        Assert.assertEquals(countTableRole(BusinessEntity.Account.getBatchStore()), numAccounts);
        Assert.assertEquals(countTableRole(BusinessEntity.Contact.getBatchStore()), numContacts);
        Assert.assertEquals(countTableRole(BusinessEntity.Product.getBatchStore()), numProducts);

        Assert.assertEquals(countInRedshift(BusinessEntity.Account), numAccounts);
        Assert.assertEquals(countInRedshift(BusinessEntity.Contact), numContacts);

        createTestSegment2();
        Map<BusinessEntity, Long> segment2Counts = ImmutableMap.of( //
                BusinessEntity.Account, 12L, BusinessEntity.Contact, 12L);  // Will change to >0 check later
        verifyTestSegment2Counts(segment2Counts);
        verifyUpdateActions();
    }

}
