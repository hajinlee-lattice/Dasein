package com.latticeengines.pls.controller;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.List;

import org.joda.time.DateTime;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.pls.S3ImportTemplateDisplay;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.pls.frontend.UIAction;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.pls.service.FileUploadService;
import com.latticeengines.pls.service.ModelingFileMetadataService;
import com.latticeengines.pls.service.SourceFileService;

public class S3TemplateDeploymentTestNG extends PlsDeploymentTestNGBase {

    private S3ImportTemplateDisplay templateDisplay;

    @Autowired
    private ModelingFileMetadataService modelingFileMetadataService;

    @Autowired
    private FileUploadService fileUploadService;

    @Autowired
    private SourceFileService sourceFileService;

    private static final String SOURCE_FILE_LOCAL_PATH = "com/latticeengines/pls/end2end/cdlCSVImport/";
    private static final String SOURCE = "File";
    private static final String FEED_TYPE_SUFFIX = "Schema";
    private static final String BASE_URL_PREFIX = "/pls/cdl";

    private static final String ENTITY_ACCOUNT = "Account";
    private static final String ACCOUNT_SOURCE_FILE = "Account_base.csv";

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG);
        MultiTenantContext.setTenant(mainTestTenant);
    }

    @Test(groups = "deployment")
    public void testCreateS3Template() throws Exception {
        assertTrue(getS3ImportTemplateEntries());
        SourceFile file = uploadSourceFile(ACCOUNT_SOURCE_FILE, ENTITY_ACCOUNT);
        String templateName = file.getName();
        templateDisplay.setTemplateName(templateName);
        String url = String.format(BASE_URL_PREFIX + "/s3/template?templateFileName=%s", templateName);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.valueOf("application/json;UTF-8"));
        HttpEntity<String> requestEntity = new HttpEntity<String>(JsonUtils.serialize(templateDisplay), headers);
        ResponseEntity<String> responseEntity = restTemplate.postForEntity(getRestAPIHostPort() + url, requestEntity,
                String.class);
        String responseBody = responseEntity.getBody();
        JSONObject jsonObject = new JSONObject(responseBody);
        UIAction uiAction = JsonUtils.deserialize(jsonObject.getString("UIAction"), UIAction.class);
        assertTrue("Success".equalsIgnoreCase(uiAction.getStatus().toString()));
    }

    @Test(groups = "deployment")
    public void testImportS3Template() throws Exception {
        assertTrue(getS3ImportTemplateEntries());
        String url = String.format(BASE_URL_PREFIX + "/s3/template/import?templateFileName=%s",
                templateDisplay.getTemplateName());
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.valueOf("application/json;UTF-8"));
        HttpEntity<String> requestEntity = new HttpEntity<String>(JsonUtils.serialize(templateDisplay), headers);
        ResponseEntity<String> responseEntity = restTemplate.postForEntity(getRestAPIHostPort() + url, requestEntity,
                String.class);
        String responseBody = responseEntity.getBody();
        JSONObject jsonObject = new JSONObject(responseBody);
        UIAction uiAction = JsonUtils.deserialize(jsonObject.getString("UIAction"), UIAction.class);
        assertTrue("Success".equalsIgnoreCase(uiAction.getStatus().toString()));
    }

    @Test(groups = "deployment", dependsOnMethods = "testCreateS3Template")
    public void testUpdateTemplateName() throws Exception {
        assertTrue(getS3ImportTemplateEntries());
        String url = BASE_URL_PREFIX + "/s3/template/displayname";
        String updateName = "test111";
        templateDisplay.setTemplateName(updateName);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.valueOf("application/json;UTF-8"));
        HttpEntity<String> requestEntity = new HttpEntity<String>(JsonUtils.serialize(templateDisplay), headers);
        restTemplate.put(getRestAPIHostPort() + url, requestEntity);
        assertTrue(getS3ImportTemplateEntries());
    }

    private SourceFile uploadSourceFile(String csvFileName, String entity) {
        SourceFile sourceFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(entity), entity, csvFileName,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + csvFileName));

        String feedType = entity + FEED_TYPE_SUFFIX;
        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(sourceFile.getName(), entity, SOURCE, feedType);
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getMappedField() == null) {
                fieldMapping.setMappedField(fieldMapping.getUserField());
                fieldMapping.setMappedToLatticeField(false);
            }
        }
        modelingFileMetadataService.resolveMetadata(sourceFile.getName(), fieldMappingDocument, entity, SOURCE,
                feedType);
        sourceFile = sourceFileService.findByName(sourceFile.getName());

        return sourceFile;
    }

    private boolean getS3ImportTemplateEntries() {
        String url = BASE_URL_PREFIX + "/s3import/template";
        boolean flag = false;
        ResponseEntity<String> responseEntity = restTemplate.getForEntity(getRestAPIHostPort() + url, String.class);
        List<S3ImportTemplateDisplay> templateDisplays = JsonUtils.deserialize(responseEntity.getBody(),
                new TypeReference<List<S3ImportTemplateDisplay>>() {
                });
        assertNotNull(templateDisplays);
        if (templateDisplay == null) {
            for (S3ImportTemplateDisplay template : templateDisplays) {
                if (template.getObject().equals(EntityType.Accounts.getDisplayName())) {
                    templateDisplay = template;
                    flag = true;
                }
            }
        } else {
            for (S3ImportTemplateDisplay template : templateDisplays) {
                if (template.getObject().equals(templateDisplay.getObject())
                        && template.getTemplateName().equals(templateDisplay.getTemplateName())) {
                    templateDisplay = template;
                    flag = true;
                    break;
                }
            }
        }
        return flag;
    }

}
