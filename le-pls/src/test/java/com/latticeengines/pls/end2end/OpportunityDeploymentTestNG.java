package com.latticeengines.pls.end2end;

import static com.latticeengines.domain.exposed.cdl.S3ImportSystem.SystemType.Salesforce;
import static org.testng.Assert.assertEquals;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.query.EntityTypeUtils;
import com.latticeengines.domain.exposed.workflow.JobStatus;

public class OpportunityDeploymentTestNG extends CSVFileImportDeploymentTestNGBase {

    private static final String TEST_SYSTEM_NAME = "FirstSystem";

    private String templateFeedType;
    private DataFeedTask opportunityDataFeedTask;
    private S3ImportSystem importSystem;

    @BeforeClass(groups = "deployment-app")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG);
        MultiTenantContext.setTenant(mainTestTenant);
        customerSpace = mainTestTenant.getId();
    }

    @Test(groups = "deployment-app")
    public void testCreateOpportunityTemplate() {
        cdlService.createS3ImportSystem(customerSpace, TEST_SYSTEM_NAME, Salesforce, true);
        createAccountTemplateAndVerify();
        importSystem = cdlService.getS3ImportSystem(customerSpace, TEST_SYSTEM_NAME);
        boolean result =  cdlService.createDefaultOpportunityTemplate(customerSpace, TEST_SYSTEM_NAME);
        Assert.assertTrue(result);
        List<S3ImportSystem> allSystems = cdlService.getAllS3ImportSystem(customerSpace);
        S3ImportSystem opportunity = allSystems.stream() //
                .filter(system -> Salesforce.equals(system.getSystemType())) //
                .findAny() //
                .orElse(null);
        Assert.assertNotNull(opportunity,
                String.format("Should exist a opportunity system. systems=%s", JsonUtils.serialize(allSystems)));
        // verification Opportunity
        templateFeedType = EntityTypeUtils.generateFullFeedType(opportunity.getName(), EntityType.Opportunity);
        opportunityDataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, "File", templateFeedType);
        Assert.assertNotNull(opportunityDataFeedTask);
        Table template = opportunityDataFeedTask.getImportTemplate();
        Assert.assertNotNull(template);
        Assert.assertNotNull(template.getAttribute(InterfaceName.StageName));
        Assert.assertNotNull(template.getAttribute(InterfaceName.LastModifiedDate));
        Assert.assertNotNull(template.getAttribute(importSystem.getAccountSystemId()));
        Assert.assertNotNull(template.getAttribute(InterfaceName.Id));

        //verification Stage
        String stageFeedType = EntityTypeUtils.generateFullFeedType(opportunity.getName(),
                EntityType.OpportunityStageName);
        DataFeedTask stageDataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, "File", stageFeedType);
        Assert.assertNotNull(stageDataFeedTask);
        template = stageDataFeedTask.getImportTemplate();
        Assert.assertNotNull(template);
        Assert.assertNotNull(template.getAttribute(InterfaceName.StageName));
    }

    private String createAccountTemplateAndVerify() {
        SourceFile defaultAccountFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_ACCOUNT), ENTITY_ACCOUNT, ACCOUNT_SOURCE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + ACCOUNT_SOURCE_FILE));
        String defaultFeedType = getFeedTypeByEntity(TEST_SYSTEM_NAME, ENTITY_ACCOUNT);

        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(defaultAccountFile.getName(), ENTITY_ACCOUNT, SOURCE, defaultFeedType);

        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getUserField().equals("CrmAccount_External_ID")) {
                fieldMapping.setIdType(FieldMapping.IdType.Account);
            }
        }

        modelingFileMetadataService.resolveMetadata(defaultAccountFile.getName(), fieldMappingDocument, ENTITY_ACCOUNT, SOURCE,
                defaultFeedType);
        defaultAccountFile = sourceFileService.findByName(defaultAccountFile.getName());

        String defaultDFId = cdlService.createS3Template(customerSpace, defaultAccountFile.getName(),
                SOURCE, ENTITY_ACCOUNT, defaultFeedType, null, ENTITY_ACCOUNT + "Data");
        Assert.assertNotNull(defaultAccountFile);
        Assert.assertNotNull(defaultDFId);

        S3ImportSystem defaultSystem = cdlService.getS3ImportSystem(mainTestTenant.getId(), TEST_SYSTEM_NAME);
        Assert.assertNotNull(defaultSystem);
        Assert.assertNotNull(defaultSystem.getAccountSystemId());
        Table defaultAccountTable = dataFeedProxy.getDataFeedTask(customerSpace, defaultDFId).getImportTemplate();
        Assert.assertNotNull(defaultAccountTable);
        return defaultFeedType;
    }

    @Test(groups = "deployment-app", dependsOnMethods = "testCreateOpportunityTemplate")
    public void testS3Import() {
        String csvFileName = "importOpportunity.csv";
        String entity = BusinessEntity.Opportunity.name();
        SourceFile sourceFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(entity), entity, csvFileName,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + csvFileName));
        ApplicationId applicationId = cdlService.submitS3ImportOnlyData(customerSpace,
                opportunityDataFeedTask.getUniqueId(), sourceFile.getName());
        JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, applicationId.toString(), false);
        assertEquals(completedStatus, JobStatus.COMPLETED);
    }
}
