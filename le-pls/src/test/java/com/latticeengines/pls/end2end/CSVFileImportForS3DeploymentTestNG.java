package com.latticeengines.pls.end2end;

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
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.util.S3PathBuilder;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;

public class CSVFileImportForS3DeploymentTestNG extends CSVFileImportDeploymentTestNGBase {

    @Inject
    private DropBoxProxy dropBoxProxy;
    private List<S3ImportTemplateDisplay> templates = null;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG);
        MultiTenantContext.setTenant(mainTestTenant);
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        templates = cdlService.getS3ImportTemplate(customerSpace, "");
    }

    @Test(groups = "deployment")
    public void importS3Base() {
        prepareS3BaseData(ENTITY_ACCOUNT, EntityType.Accounts);
        prepareS3BaseData(ENTITY_CONTACT, EntityType.Contacts);
        prepareS3BaseData(ENTITY_TRANSACTION, EntityType.ProductPurchases);
    }

    private void prepareS3BaseData(String entity, EntityType entityType) {
        switch (entity) {
        case ENTITY_ACCOUNT:
            testS3ImportWithTemplateData(ACCOUNT_SOURCE_FILE, ENTITY_ACCOUNT, entityType);
            testS3ImportOnlyData(ACCOUNT_SOURCE_FILE, ENTITY_ACCOUNT);
            break;
        case ENTITY_CONTACT:
            testS3ImportWithTemplateData(CONTACT_SOURCE_FILE, ENTITY_CONTACT, entityType);
            testS3ImportOnlyData(CONTACT_SOURCE_FILE, ENTITY_CONTACT);
            break;
        case ENTITY_TRANSACTION:
            testS3ImportWithTemplateData(TRANSACTION_SOURCE_FILE, ENTITY_TRANSACTION, entityType);
            testS3ImportOnlyData(TRANSACTION_SOURCE_FILE, ENTITY_TRANSACTION);
            break;
        }
    }

    private void testS3ImportWithTemplateData(String csvFileName, String entity, EntityType entityType) {
        SourceFile sourceFile = uploadSourceFile(csvFileName, entity);
        String subType = entityType.getSubType() != null ? entityType.getSubType().name() : null;
        String taskId = cdlService.createS3Template(customerSpace, sourceFile.getName(), SOURCE, entity,
                getFeedTypeByEntity(DEFAULT_SYSTEM, entity), subType, entityType.getDisplayName());
        ApplicationId applicationId = cdlService.submitS3ImportWithTemplateData(customerSpace, taskId,
                sourceFile.getName());
        JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, applicationId.toString(), false);
        assertEquals(completedStatus, JobStatus.COMPLETED);

    }

    private void testS3ImportOnlyData(String csvFileName, String entity) {
        SourceFile sourceFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(entity), entity, csvFileName,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + csvFileName));
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, SOURCE,
                getFeedTypeByEntity(DEFAULT_SYSTEM, entity));
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
        List<String> feedTypes = Arrays.asList(getFeedTypeByEntity(DEFAULT_SYSTEM, ENTITY_ACCOUNT),
                getFeedTypeByEntity(DEFAULT_SYSTEM, ENTITY_CONTACT),
                getFeedTypeByEntity(DEFAULT_SYSTEM, ENTITY_TRANSACTION));
        // S3ImportTemplateDisplay display = templates.get(0);
        // Assert.assertEquals(display.getPath(), "N/A");
        DropBoxSummary dropBoxSummary = dropBoxProxy.getDropBox(customerSpace);
        for (S3ImportTemplateDisplay display : templates) {
            Assert.assertEquals(display.getPath(), S3PathBuilder.getUiDisplayS3Dir(dropBoxSummary.getBucket(),
                    dropBoxSummary.getDropBox(), display.getFeedType()));
        }
    }

}
