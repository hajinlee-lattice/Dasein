package com.latticeengines.apps.cdl.qaend2end;

import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLQATestNGBase;
import com.latticeengines.domain.exposed.query.EntityType;

public class ProcessAnalyzeImportCorrectnessQAEnd2EndTestNG extends CDLQATestNGBase {
    private static final Logger log = LoggerFactory.getLogger(ProcessAnalyzeImportCorrectnessQAEnd2EndTestNG.class);
    private static final Integer DEFAULT_WAIT_PA_READY_IN_MINUTES = 60;

    @Value("${pa.import.correctness.cleanup.tenant}")
    private boolean needCleanupTenant;

    @Value("${pa.import.correctness.account}")
    private String accountFilePath;

    @Value("${pa.import.correctness.contact}")
    private String contactFilePath;

    @Value("${pa.import.correctness.transaction}")
    private String transactionFilePath;

    @Value("${pa.import.correctness.bundle}")
    private String productBundleFilePath;

    @Value("${pa.import.correctness.hierarchy}")
    private String productHierarchyFilePath;

    @Override
    protected void checkBasicInfo() {
        super.checkBasicInfo();
        Assert.assertTrue(StringUtils.isNotEmpty(accountFilePath), "Account file is required");
        Assert.assertTrue(StringUtils.isNotEmpty(contactFilePath), "Contact file is required");
        Assert.assertTrue(StringUtils.isNotEmpty(transactionFilePath), "Transaction file is required");
        Assert.assertTrue(StringUtils.isNotEmpty(productBundleFilePath), "Product bundle file is required");
        Assert.assertTrue(StringUtils.isNotEmpty(productHierarchyFilePath), "Product hierarchy file is required");
    }

    @Test(groups = { "qaend2end" })
    public void runTest() throws TimeoutException {
        // cleanup tenant
        if (needCleanupTenant) {
            cleanupTenant();
        }

        // file import
        log.info("Starting file import...");
        testFileImportService.upsertDefaultTemplateByAutoMapping(accountFilePath, EntityType.Accounts, true);
        testFileImportService.upsertDefaultTemplateByAutoMapping(contactFilePath, EntityType.Contacts, true);
        testFileImportService.upsertDefaultTemplateByAutoMapping(transactionFilePath, EntityType.ProductPurchases,
                true);
        testFileImportService.upsertDefaultTemplateByAutoMapping(productBundleFilePath, EntityType.ProductBundles,
                true);
        testFileImportService.upsertDefaultTemplateByAutoMapping(productHierarchyFilePath, EntityType.ProductHierarchy,
                true);

        // wait all file import actions are done
        log.info("Waiting all file import actions are done...");
        testJobService.waitForProcessAnalyzeAllActionsDone(DEFAULT_WAIT_PA_READY_IN_MINUTES);

        // run PA
        log.info("Starting PA for file import...");
        testJobService.processAnalyzeRunNow(mainTestTenant);
    }
}
