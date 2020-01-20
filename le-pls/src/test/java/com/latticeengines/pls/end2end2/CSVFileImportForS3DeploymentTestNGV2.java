package com.latticeengines.pls.end2end2;

import static org.testng.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.S3ImportTemplateDisplay;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.query.EntityTypeUtils;
import com.latticeengines.domain.exposed.util.S3PathBuilder;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;

public class CSVFileImportForS3DeploymentTestNGV2 extends CSVFileImportDeploymentTestNGBaseV2 {

    @Inject
    private DropBoxProxy dropBoxProxy;
    private List<S3ImportTemplateDisplay> templates = null;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG);
        MultiTenantContext.setTenant(mainTestTenant);
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        templates = cdlService.getS3ImportTemplate(customerSpace, "", null);
    }

    @Test(groups = "deployment")
    public void importS3Base() throws Exception {
        prepareS3BaseData(EntityType.Accounts);
        prepareS3BaseData(EntityType.Contacts);
        prepareS3BaseData(EntityType.ProductPurchases);
    }

    private void prepareS3BaseData(EntityType entityType) throws Exception {
        switch (entityType) {
            case Accounts:
            testS3ImportWithTemplateData(ACCOUNT_SOURCE_FILE, entityType);
            testS3ImportOnlyData(ACCOUNT_SOURCE_FILE, entityType);
            break;
            case Contacts:
            testS3ImportWithTemplateData(CONTACT_SOURCE_FILE, entityType);
            testS3ImportOnlyData(CONTACT_SOURCE_FILE, entityType);
            break;
            case ProductPurchases:
            testS3ImportWithTemplateData(TRANSACTION_SOURCE_FILE, entityType);
            testS3ImportOnlyData(TRANSACTION_SOURCE_FILE, entityType);
            break;
        }
    }

    private void testS3ImportWithTemplateData(String csvFileName, EntityType entityType) throws Exception {
        SourceFile sourceFile = uploadSourceFile(DEFAULT_SYSTEM, DEFAULT_SYSTEM_TYPE, entityType,
                csvFileName);
        String subType = entityType.getSubType() != null ? entityType.getSubType().name() : null;
        String taskId = cdlService.createS3Template(customerSpace, sourceFile.getName(), SOURCE,
                entityType.getEntity().name(),
                EntityTypeUtils.generateFullFeedType(DEFAULT_SYSTEM, entityType), subType, entityType.getDisplayName());
        ApplicationId applicationId = cdlService.submitS3ImportWithTemplateData(customerSpace, taskId,
                sourceFile.getName());
        JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, applicationId.toString(), false);
        assertEquals(completedStatus, JobStatus.COMPLETED);

    }

    private void testS3ImportOnlyData(String csvFileName, EntityType entityType) {
        SourceFile sourceFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                entityType.getSchemaInterpretation(), entityType.getEntity().name(), csvFileName,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + csvFileName));
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, SOURCE,
                EntityTypeUtils.generateFullFeedType(DEFAULT_SYSTEM, entityType));
        ApplicationId applicationId = cdlService.submitS3ImportOnlyData(customerSpace, dataFeedTask.getUniqueId(),
                sourceFile.getName());
        JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, applicationId.toString(), false);
        assertEquals(completedStatus, JobStatus.COMPLETED);
    }

    @Test(groups = "deployment", dependsOnMethods = "importS3Base")
    public void testGetS3ImportDisplay() {
        // verify that the tenant has 5 template display by default
        Assert.assertNotNull(templates);
        Assert.assertEquals(templates.size(), 5);
        List<String> feedTypes = Arrays.asList(
                EntityTypeUtils.generateFullFeedType(DEFAULT_SYSTEM, EntityType.Accounts),
                EntityTypeUtils.generateFullFeedType(DEFAULT_SYSTEM, EntityType.Contacts),
                EntityTypeUtils.generateFullFeedType(DEFAULT_SYSTEM, EntityType.ProductPurchases));
        // S3ImportTemplateDisplay display = templates.get(0);
        // Assert.assertEquals(display.getPath(), "N/A");
        DropBoxSummary dropBoxSummary = dropBoxProxy.getDropBox(customerSpace);
        for (S3ImportTemplateDisplay display : templates) {
            Assert.assertEquals(display.getPath(), S3PathBuilder.getUiDisplayS3Dir(dropBoxSummary.getBucket(),
                    dropBoxSummary.getDropBox(), display.getFeedType()));
        }
    }

}
