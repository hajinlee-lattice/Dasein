package com.latticeengines.testframework.service.impl;

import java.util.List;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.FileSystemResource;
import org.springframework.stereotype.Service;

import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.pls.S3ImportTemplateDisplay;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.query.EntityTypeUtils;
import com.latticeengines.testframework.exposed.proxy.pls.ModelingFileUploadProxy;
import com.latticeengines.testframework.exposed.proxy.pls.PlsCDLS3ImportProxy;
import com.latticeengines.testframework.exposed.proxy.pls.PlsCDLS3TemplateProxy;
import com.latticeengines.testframework.exposed.service.TestFileImportService;

@Service("testFileImportServiceImpl")
public class TestFileImportServiceImpl implements TestFileImportService {

    private static final Logger log = LoggerFactory.getLogger(TestFileImportServiceImpl.class);
    private static final String DEFAULT_SYSTEM = "DefaultSystem";

    @Resource(name = "deploymentTestBed")
    private GlobalAuthDeploymentTestBed deploymentTestBed;

    @Inject
    private PlsCDLS3TemplateProxy plsCDLS3TemplateProxy;

    @Inject
    private PlsCDLS3ImportProxy plsCDLS3ImportProxy;

    @Inject
    private ModelingFileUploadProxy modelingFileUploadProxy;

    @Override
    public List<S3ImportTemplateDisplay> getAllTemplates() {
        deploymentTestBed.attachProtectedProxy(plsCDLS3ImportProxy);
        return plsCDLS3ImportProxy.getS3ImportTemplateEntries();
    }

    @Override
    public S3ImportTemplateDisplay getTemplate(String feedType, String templateName) {
        return getAllTemplates().stream().filter(
                template -> template.getFeedType().equals(feedType) && template.getTemplateName().equals(templateName))
                .findFirst().orElse(null);
    }

    @Override
    public SourceFile uploadFile(String filePath, EntityType entity) {
        deploymentTestBed.attachProtectedProxy(modelingFileUploadProxy);
        return modelingFileUploadProxy.uploadFile(String.format("file_%s.csv", System.currentTimeMillis()), false,
                FilenameUtils.getName(filePath), entity.getEntity().name(), new FileSystemResource(filePath));
    }

    @Override
    public FieldMappingDocument getFieldMappings(String sourceFileName, EntityType entity, SourceType source,
            String feedType) {
        deploymentTestBed.attachProtectedProxy(modelingFileUploadProxy);
        if (StringUtils.isEmpty(feedType)) {
            feedType = EntityTypeUtils.generateFullFeedType(DEFAULT_SYSTEM, entity);
        }
        return modelingFileUploadProxy.getFieldMappings(sourceFileName, entity.getEntity().name(), source.getName(), feedType);
    }

    @Override
    public void saveFieldMappingDocument(String displayName, FieldMappingDocument fieldMappingDocument,
            EntityType entity, SourceType source, String feedType) {
        deploymentTestBed.attachProtectedProxy(modelingFileUploadProxy);
        if (StringUtils.isEmpty(feedType)) {
            feedType = EntityTypeUtils.generateFullFeedType(DEFAULT_SYSTEM, entity);
        }
        modelingFileUploadProxy.saveFieldMappingDocument(displayName, fieldMappingDocument, entity.getEntity().name(),
                source.getName(), feedType);
    }

    private String getTemplateNameByFeedType(String feedType) {
        return getAllTemplates().stream().filter(template -> template.getFeedType().equals(feedType)).findFirst()
                .orElseThrow(() -> new RuntimeException("The template name cannot be found by feed type " + feedType))
                .getTemplateName();
    }

    @Override
    public void createS3Template(String templateFileName, SourceType source, boolean importData,
            S3ImportTemplateDisplay templateDisplay) {
        deploymentTestBed.attachProtectedProxy(plsCDLS3TemplateProxy);
        plsCDLS3TemplateProxy.createS3Template(templateFileName, source.getName(), importData, templateDisplay);
    }

    @Override
    public void upsertTemplateByAutoMapping(String filePath, String templateName, String feedType, EntityType entity,
            boolean importData, S3ImportTemplateDisplay template) {
        log.info("Uploading file {}", filePath);
        SourceFile sourceFile = uploadFile(filePath, entity);

        log.info("Getting field mappings for uploaded file {}", sourceFile.getName());
        FieldMappingDocument fieldMappingDocument = getFieldMappings(sourceFile.getName(), entity, SourceType.FILE,
                feedType);

        log.info("Saving field mappings for uploaded file {}", sourceFile.getName());
        saveFieldMappingDocument(sourceFile.getName(), fieldMappingDocument, entity, SourceType.FILE, feedType);

        log.info("Creating or updating template {} with feedType {}", templateName, feedType);
        createS3Template(sourceFile.getName(), SourceType.FILE, importData, template);
    }

    @Override
    public void upsertDefaultTemplateByAutoMapping(String filePath, EntityType entity, boolean importData) {
        String defaultFeedType = EntityTypeUtils.generateFullFeedType(DEFAULT_SYSTEM, entity);
        String defaultTemplateName = getTemplateNameByFeedType(defaultFeedType);
        S3ImportTemplateDisplay defaultTemplate = getTemplate(defaultFeedType, defaultTemplateName);
        if (defaultTemplate == null) {
            throw new RuntimeException(String.format("Not able to find default template %s, with feedType %s",
                    defaultTemplateName, defaultFeedType));
        }
        upsertTemplateByAutoMapping(filePath, defaultTemplateName, defaultFeedType, entity, importData,
                defaultTemplate);
    }
}
