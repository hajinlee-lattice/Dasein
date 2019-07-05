package com.latticeengines.pls.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.pls.service.CDLService;
import com.latticeengines.pls.service.FileUploadService;
import com.latticeengines.pls.service.ModelingFileMetadataService;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

public class CDLServiceImplDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(CDLServiceImplDeploymentTestNG.class);

    public static final String COLLECTION_DATE_FORMAT = "yyyy-MM-dd-HH-mm-ss";

    private static final String FILE_DISPLAY_NAME = "cdlImportCSV_data.csv";

    private static final String TEMPLATE_NAME = "cdlImportCSV_template.csv";

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private FileUploadService fileUploadService;

    @Autowired
    private CDLService cdlService;

    @Autowired
    private ModelingFileMetadataService modelingFileMetadataService;

    @Autowired
    private WorkflowProxy workflowProxy;

    private SourceFile template;

    private SourceFile data;

    private Tenant tenant;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        String featureFlag = LatticeFeatureFlag.LATTICE_INSIGHTS.getName();
        Map<String, Boolean> flags = new HashMap<>();
        flags.put(featureFlag, true);
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3, flags);
        tenant = testBed.getMainTestTenant();
        testBed.loginAndAttach(TestFrameworkUtils.usernameForAccessLevel(AccessLevel.SUPER_ADMIN),
                TestFrameworkUtils.GENERAL_PASSWORD, tenant);
        MultiTenantContext.setTenant(tenant);
        File templateFile = new File(
                ClassLoader.getSystemResource("com/latticeengines/pls/service/impl/" + TEMPLATE_NAME).getPath());

        File dataFile = new File(
                ClassLoader.getSystemResource("com/latticeengines/pls/service/impl/" + FILE_DISPLAY_NAME).getPath());

        template = fileUploadService.uploadFile(TEMPLATE_NAME, SchemaInterpretation.Account, null, null,
                new FileInputStream(templateFile));
        data = fileUploadService.uploadFile(FILE_DISPLAY_NAME, SchemaInterpretation.Account, null, FILE_DISPLAY_NAME,
                new FileInputStream(dataFile));

        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(template.getName(), SchemaInterpretation.Account, null, false, false,
                        false);
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getMappedField() == null) {
                fieldMapping.setMappedField(fieldMapping.getUserField());
                fieldMapping.setMappedToLatticeField(true);
            }
        }
        modelingFileMetadataService.resolveMetadata(template.getName(), fieldMappingDocument, false, false);
    }

    @Test(groups = "deployment")
    public void testS3ImportSystem() {
        String customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        cdlService.createS3ImportSystem(customerSpace, "SYSTEM1", S3ImportSystem.SystemType.Salesforce, false);
        cdlService.createS3ImportSystem(customerSpace, "SYSTEM2", S3ImportSystem.SystemType.Other, false);

        List<S3ImportSystem> allSystem = cdlService.getAllS3ImportSystem(customerSpace);
        for (S3ImportSystem system : allSystem) {
            if (system.getName().equals("SYSTEM1")) {
                Assert.assertEquals(system.getSystemType(), S3ImportSystem.SystemType.Salesforce);
                Assert.assertTrue(system.isPrimarySystem());
            } else {
                Assert.assertEquals(system.getSystemType(), S3ImportSystem.SystemType.Other);
                Assert.assertEquals(system.getPriority(), 2);
            }
        }
        cdlService.createS3ImportSystem(customerSpace, "PRIMARY SYSTEM", S3ImportSystem.SystemType.Other, true);
        allSystem = cdlService.getAllS3ImportSystem(customerSpace);
        Assert.assertEquals(allSystem.size(), 3);
        boolean hasPrimary = false;
        for (S3ImportSystem system : allSystem) {
            if (system.getDisplayName().equals("PRIMARY SYSTEM")) {
                hasPrimary = true;
                Assert.assertTrue(system.isPrimarySystem());
            }
        }
        Assert.assertTrue(hasPrimary);
        Assert.assertThrows(RuntimeException.class,
                () -> cdlService.createS3ImportSystem(customerSpace, "SYSTEM1", S3ImportSystem.SystemType.Other,
                        false));
    }

    @Test(groups = "deployment", enabled = false)
    public void testImportJob() throws Exception {
        long startMillis = System.currentTimeMillis();
        ApplicationId appId = cdlService.submitCSVImport(CustomerSpace.parse(tenant.getName()).toString(),
                template.getName(), data.getName(), "File", "Account", "test");
        Assert.assertNotNull(appId);
        JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, appId.toString(), false);
        assertEquals(completedStatus, JobStatus.COMPLETED);
        Job job = workflowProxy.getWorkflowJobFromApplicationId(appId.toString());
        Assert.assertNotNull(job);
        Assert.assertTrue(job.getInputs().containsKey(WorkflowContextConstants.Inputs.SOURCE_DISPLAY_NAME));
        Assert.assertTrue(job.getInputs().containsKey(WorkflowContextConstants.Inputs.SOURCE_FILE_NAME));
        Assert.assertEquals(job.getInputs().get(WorkflowContextConstants.Inputs.SOURCE_DISPLAY_NAME),
                FILE_DISPLAY_NAME);
        log.info(String.format("fileName=%s", job.getInputs().get(WorkflowContextConstants.Inputs.SOURCE_FILE_NAME)));
        long endMillis = System.currentTimeMillis();
        checkExtractFolderExist(startMillis, endMillis);
    }

    private void checkExtractFolderExist(long startMillis, long endMillis) throws Exception {
        String targetPath = String.format("%s/%s/DataFeed1/DataFeed1-Account/Extracts", PathBuilder
                .buildDataTablePath(CamilleEnvironment.getPodId(), CustomerSpace.parse(tenant.getId())).toString(),
                SourceType.FILE.getName());
        assertTrue(HdfsUtils.fileExists(yarnConfiguration, targetPath));
        List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, targetPath);
        for (String file : files) {
            String filename = file.substring(file.lastIndexOf("/") + 1);
            Date folderTime = new SimpleDateFormat(COLLECTION_DATE_FORMAT).parse(filename);
            if (folderTime.getTime() > startMillis && folderTime.getTime() < endMillis) {
                log.info("Find matched file: " + filename);
                return;
            }
        }
        fail("No data collection folder was created!");
    }
}
