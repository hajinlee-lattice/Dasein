package com.latticeengines.pls.controller.dcp;

import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.SourceFileInfo;
import com.latticeengines.domain.exposed.dcp.SourceRequest;
import com.latticeengines.domain.exposed.dcp.UpdateSourceRequest;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.frontend.FetchFieldDefinitionsResponse;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinition;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.pls.frontend.ValidateFieldDefinitionsRequest;
import com.latticeengines.domain.exposed.pls.frontend.ValidateFieldDefinitionsResponse;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.pls.functionalframework.DCPDeploymentTestNGBase;
import com.latticeengines.testframework.exposed.proxy.pls.FileUploadProxy;
import com.latticeengines.testframework.exposed.proxy.pls.TestProjectProxy;
import com.latticeengines.testframework.exposed.proxy.pls.TestSourceProxy;


public class SourceResourceDeploymentTestNG extends DCPDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(SourceResourceDeploymentTestNG.class);

    @Inject
    private FileUploadProxy fileUploadProxy;

    @Inject
    private TestProjectProxy testProjectProxy;

    @Inject
    private TestSourceProxy testSourceProxy;


    private String sourceId;
    private String fileImportId;

    @BeforeClass(groups = "deployment-dcp")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.DCP);
        MultiTenantContext.setTenant(mainTestTenant);
        attachProtectedProxy(testProjectProxy);
        attachProtectedProxy(testSourceProxy);
        attachProtectedProxy(fileUploadProxy);
    }

    @Test(groups = "deployment-dcp")
    public void testCreateAndGetSource() {
        ProjectDetails projectDetail = testProjectProxy.createProjectWithOutProjectId("testProject",
                Project.ProjectType.Type1, null);
        Assert.assertNotNull(projectDetail);
        String projectId = projectDetail.getProjectId();

        InputStream specStream = testArtifactService.readTestArtifactAsStream(TEST_TEMPLATE_DIR, TEST_TEMPLATE_VERSION,
                TEST_TEMPLATE_NAME);

        FieldDefinitionsRecord fieldDefinitionsRecord = JsonUtils.deserialize(specStream, FieldDefinitionsRecord.class);

        SourceRequest sourceRequest = new SourceRequest();
        sourceRequest.setProjectId(projectId);
        sourceRequest.setDisplayName("testSource");
        sourceRequest.setFieldDefinitionsRecord(fieldDefinitionsRecord);
        Source source = testSourceProxy.createSource(sourceRequest);
        Assert.assertNotNull(source);
        Assert.assertEquals(source.getImportStatus(), DataFeedTask.S3ImportStatus.Active);
        Assert.assertFalse(StringUtils.isBlank(source.getSourceId()));
        Assert.assertFalse(StringUtils.isBlank(source.getDropFullPath()));

        sourceRequest.setDisplayName("testSource2");
        Source source2 = testSourceProxy.createSource(sourceRequest);

        Assert.assertNotEquals(source.getSourceId(), source2.getSourceId());

        testSourceProxy.pauseSourceById(source.getSourceId());
        Source getSource = testSourceProxy.getSource(source.getSourceId());
        Assert.assertNotNull(getSource);
        Assert.assertEquals(getSource.getSourceId(), source.getSourceId());
        Assert.assertEquals(getSource.getImportStatus(), DataFeedTask.S3ImportStatus.Pause);

        testSourceProxy.reactivateSourceById(source.getSourceId());
        getSource = testSourceProxy.getSource(source.getSourceId());
        Assert.assertNotNull(getSource);
        Assert.assertEquals(getSource.getSourceId(), source.getSourceId());
        Assert.assertEquals(getSource.getImportStatus(), DataFeedTask.S3ImportStatus.Active);

        List<Source> allSources = testSourceProxy.getSourcesByProject(projectId);
        Assert.assertNotNull(allSources);
        Assert.assertEquals(allSources.size(), 2);
        Set<String> allIds = new HashSet<>(Arrays.asList(source.getSourceId(),  source2.getSourceId()));
        allSources.forEach(s -> {
            Assert.assertTrue(allIds.contains(s.getSourceId()));
            Assert.assertFalse(StringUtils.isEmpty(s.getDropFullPath()));
        });

        testSourceProxy.deleteSourceById(source.getSourceId());

        allSources = testSourceProxy.getSourcesByProject(projectId);
        Assert.assertEquals(allSources.size(), 1);
        Assert.assertEquals(allSources.get(0).getSourceId(), source2.getSourceId());
        sourceId = source2.getSourceId();
    }

    @Test(groups = "deployment-dcp", dependsOnMethods = "testCreateAndGetSource")
    public void testFieldDefinitions() {
        Resource csvResource = new MultipartFileResource(testArtifactService.readTestArtifactAsStream(TEST_DATA_DIR,
                TEST_DATA_VERSION, TEST_ACCOUNT_DATA_FILE), TEST_ACCOUNT_DATA_FILE);
        SourceFileInfo testSourceFile = fileUploadProxy.uploadFile(TEST_ACCOUNT_DATA_FILE, csvResource);
        FetchFieldDefinitionsResponse fetchResponse = testSourceProxy.getSourceMappings(null,
                EntityType.Accounts.name(),
                testSourceFile.getFileImportId());

        Assert.assertTrue(MapUtils.isEmpty(fetchResponse.getExistingFieldDefinitionsMap()));

        fetchResponse = testSourceProxy.getSourceMappings(sourceId,
                EntityType.Accounts.name(),
                testSourceFile.getFileImportId());
        Assert.assertTrue(MapUtils.isNotEmpty(fetchResponse.getExistingFieldDefinitionsMap()));

        ValidateFieldDefinitionsRequest validateRequest = new ValidateFieldDefinitionsRequest();
        validateRequest.setCurrentFieldDefinitionsRecord(fetchResponse.getCurrentFieldDefinitionsRecord());
        validateRequest.setExistingFieldDefinitionsMap(fetchResponse.getExistingFieldDefinitionsMap());
        validateRequest.setAutodetectionResultsMap(fetchResponse.getAutodetectionResultsMap());
        validateRequest.setImportWorkflowSpec(fetchResponse.getImportWorkflowSpec());

        ValidateFieldDefinitionsResponse response =
                testSourceProxy.validateSourceMappings(testSourceFile.getFileImportId(),
               null, validateRequest);

        Assert.assertNotEquals(response.getValidationResult(), ValidateFieldDefinitionsResponse.ValidationResult.ERROR);

        // get source mappings for source
        fetchResponse = testSourceProxy.getSourceMappings(sourceId, null, null);
        FieldDefinitionsRecord record = fetchResponse.getCurrentFieldDefinitionsRecord();
        Assert.assertNotNull(fetchResponse.getCurrentFieldDefinitionsRecord());
        log.info("test get mappings ");

        UpdateSourceRequest updateSourceRequest = new UpdateSourceRequest();
        updateSourceRequest.setDisplayName("testSourceAfterUpdate");
        updateSourceRequest.setFieldDefinitionsRecord(validateRequest.getCurrentFieldDefinitionsRecord());
        updateSourceRequest.setFileImportId(testSourceFile.getFileImportId());
        updateSourceRequest.setSourceId(sourceId);
        Source retrievedSource = testSourceProxy.updateSource(updateSourceRequest);
        Assert.assertNotNull(retrievedSource);

        log.info("test get mappings after updates");
        fetchResponse = testSourceProxy.getSourceMappings(sourceId, null, null);
        FieldDefinitionsRecord updatedRecord = fetchResponse.getCurrentFieldDefinitionsRecord();
        for (String  fieldSection : record.getFieldDefinitionsRecordsMap().keySet()) {
            List<FieldDefinition> definitions = record.getFieldDefinitionsRecords(fieldSection);
            List<FieldDefinition> updatedDefinitions = updatedRecord.getFieldDefinitionsRecords(fieldSection);
            Assert.assertNotNull(updatedDefinitions);
            Map<String, FieldDefinition> nameToFieldDefinition =
                    updatedDefinitions.stream().collect(Collectors.toMap(FieldDefinition::getFieldName, e -> e));
            definitions.forEach(e -> Assert.assertNotNull(nameToFieldDefinition.get(e.getFieldName())));
        }
    }

    @Test(groups = "deployment-dcp", dependsOnMethods = "testFieldDefinitions")
    public void testCreateSourceFromFile() {
        // create project
        ProjectDetails projectDetail = testProjectProxy.createProjectWithOutProjectId("testProject_2",
                Project.ProjectType.Type1, null);
        Assert.assertNotNull(projectDetail);
        String projectId = projectDetail.getProjectId();

        // upload test file; get fieldDefinitionsRecord for test file
        Resource csvResource = new MultipartFileResource(testArtifactService.readTestArtifactAsStream(TEST_DATA_DIR,
                TEST_DATA_VERSION, TEST_ACCOUNT_DATA_FILE), TEST_ACCOUNT_DATA_FILE);
        SourceFileInfo testSourceFile = fileUploadProxy.uploadFile(TEST_ACCOUNT_DATA_FILE, csvResource);
        fileImportId = testSourceFile.getFileImportId();

        InputStream specStream = testArtifactService.readTestArtifactAsStream(TEST_TEMPLATE_DIR, TEST_TEMPLATE_VERSION,
                TEST_TEMPLATE_NAME);
        FieldDefinitionsRecord fieldDefinitionsRecord = JsonUtils.deserialize(specStream, FieldDefinitionsRecord.class);

        // create source
        SourceRequest sourceRequest = new SourceRequest();
        sourceRequest.setDisplayName("testSource");
        sourceRequest.setProjectId(projectId);
        sourceRequest.setFileImportId(fileImportId);
        sourceRequest.setFieldDefinitionsRecord(fieldDefinitionsRecord);
        Source source = testSourceProxy.createSource(sourceRequest);
        sourceId = source.getSourceId();

        Assert.assertNotNull(source);
        Assert.assertEquals(source.getImportStatus(), DataFeedTask.S3ImportStatus.Active);
        Assert.assertFalse(StringUtils.isBlank(sourceId));
        Assert.assertFalse(StringUtils.isBlank(source.getDropFullPath()));

        // verify source retrieval from project
        List<Source> allSources = testSourceProxy.getSourcesByProject(projectId);
        Assert.assertNotNull(allSources);
        Assert.assertEquals(allSources.size(), 1);

        Source retrieved = allSources.get(0);
        Assert.assertEquals(retrieved.getSourceId(), sourceId);
        Assert.assertFalse(StringUtils.isEmpty(retrieved.getDropFullPath()));

        testSourceProxy.deleteSourceById(sourceId);
    }

    @Test(groups = "deployment-dcp", dependsOnMethods = "testCreateSourceFromFile")
    public void testUpdateSource() {
        // fetch from previous project/source
        FetchFieldDefinitionsResponse fetchResponse = testSourceProxy.getSourceMappings(sourceId,
                EntityType.Accounts.name(),
                fileImportId);
        Assert.assertTrue(MapUtils.isNotEmpty(fetchResponse.getExistingFieldDefinitionsMap()));

        // validate (no change); assert PASS
        ValidateFieldDefinitionsRequest validateRequest = new ValidateFieldDefinitionsRequest();
        validateRequest.setCurrentFieldDefinitionsRecord(fetchResponse.getCurrentFieldDefinitionsRecord());
        validateRequest.setExistingFieldDefinitionsMap(fetchResponse.getExistingFieldDefinitionsMap());
        validateRequest.setAutodetectionResultsMap(fetchResponse.getAutodetectionResultsMap());
        validateRequest.setImportWorkflowSpec(fetchResponse.getImportWorkflowSpec());
        ValidateFieldDefinitionsResponse response =
                testSourceProxy.validateSourceMappings(fileImportId,
                        EntityType.Accounts.name(), validateRequest);
        Assert.assertEquals(response.getValidationResult(), ValidateFieldDefinitionsResponse.ValidationResult.PASS);

        // make INVALID change to FieldDefinitionsRecord
        FieldDefinitionsRecord record = validateRequest.getCurrentFieldDefinitionsRecord();
        FieldDefinition fd = record.getFieldDefinition("companyInformation","CompanyName");
        Assert.assertNotNull(fd);
        fd.setFieldType(UserDefinedType.BOOLEAN);

        // updateSource with invalid FieldDefinitionsRecord, new displayName
        UpdateSourceRequest updateSourceRequest = new UpdateSourceRequest();
        updateSourceRequest.setDisplayName("updatedSource");
        updateSourceRequest.setFieldDefinitionsRecord(record);
        updateSourceRequest.setFileImportId(fileImportId);
        updateSourceRequest.setSourceId(sourceId);
        Source updatedSource = testSourceProxy.updateSource(updateSourceRequest);

        Assert.assertNotNull(updatedSource);
        Assert.assertEquals(updatedSource.getSourceDisplayName(), "updatedSource");

        // fetch updatedSource; assert FieldDefinitionsRecord changes persist
        fetchResponse = testSourceProxy.getSourceMappings(sourceId,
                EntityType.Accounts.name(),
                fileImportId);
        fd = fetchResponse.getCurrentFieldDefinitionsRecord().getFieldDefinition(
                "companyInformation", "CompanyName");

        Assert.assertTrue(MapUtils.isNotEmpty(fetchResponse.getExistingFieldDefinitionsMap()));
        Assert.assertEquals(fd.getFieldType(), UserDefinedType.BOOLEAN);

        // validate updatedSource (invalid change); assert ERROR
        validateRequest = new ValidateFieldDefinitionsRequest();
        validateRequest.setCurrentFieldDefinitionsRecord(fetchResponse.getCurrentFieldDefinitionsRecord());
        validateRequest.setExistingFieldDefinitionsMap(fetchResponse.getExistingFieldDefinitionsMap());
        validateRequest.setAutodetectionResultsMap(fetchResponse.getAutodetectionResultsMap());
        validateRequest.setImportWorkflowSpec(fetchResponse.getImportWorkflowSpec());
        response = testSourceProxy.validateSourceMappings(fileImportId,
                EntityType.Accounts.name(), validateRequest);

        Assert.assertEquals(response.getValidationResult(), ValidateFieldDefinitionsResponse.ValidationResult.ERROR);
    }
}
