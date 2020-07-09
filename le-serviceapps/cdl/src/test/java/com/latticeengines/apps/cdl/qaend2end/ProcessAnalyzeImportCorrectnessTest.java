package com.latticeengines.apps.cdl.qaend2end;

import java.util.concurrent.TimeoutException;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.Test;

import com.google.common.base.Preconditions;
import com.latticeengines.apps.cdl.testframework.CDLQATestNGBase;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.testframework.exposed.service.TestFileImportService;
import com.latticeengines.testframework.exposed.service.TestJobService;

public class ProcessAnalyzeImportCorrectnessTest extends CDLQATestNGBase {
    private static final Logger log = LoggerFactory.getLogger(ProcessAnalyzeImportCorrectnessTest.class);
    private static final Integer DEFAULT_WAIT_PA_READY_IN_MINUTES = 60;

    @Value("${pa.importcorrectness.cleanuptenant}")
    private boolean needCleanupTenant;

    @Value("${pa.importcorrectness.account}")
    private String accountFilePath;

    @Value("${pa.importcorrectness.contact}")
    private String contactFilePath;

    @Value("${pa.importcorrectness.transaction}")
    private String transactionFilePath;

    @Value("${pa.importcorrectness.bundle}")
    private String productBundleFilePath;

    @Value("${pa.importcorrectness.hierarchy}")
    private String productHierarchyFilePath;

    @Inject
    private TestFileImportService testFileImportService;

    @Inject
    private TestJobService testJobService;

    @Override
    protected void checkBasicInfo() {
        super.checkBasicInfo();
        Preconditions.checkState(StringUtils.isNotEmpty(accountFilePath), "Account file is required");
        Preconditions.checkState(StringUtils.isNotEmpty(contactFilePath), "Contact file is required");
        Preconditions.checkState(StringUtils.isNotEmpty(transactionFilePath), "Transaction file is required");
        Preconditions.checkState(StringUtils.isNotEmpty(productBundleFilePath), "Product bundle file is required");
        Preconditions.checkState(StringUtils.isNotEmpty(productHierarchyFilePath),
                "Product hierarchy file is required");
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
        testJobService.waitForProcessAnalyzeReady(DEFAULT_WAIT_PA_READY_IN_MINUTES);

        // run PA
        log.info("Starting PA for file import...");
        processAnalyzeRunNow();
    }
}
