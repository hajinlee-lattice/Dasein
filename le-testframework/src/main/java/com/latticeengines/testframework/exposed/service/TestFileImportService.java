package com.latticeengines.testframework.exposed.service;

import java.util.List;

import org.springframework.core.io.Resource;

import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.pls.S3ImportTemplateDisplay;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.query.EntityType;

public interface TestFileImportService {
    List<S3ImportTemplateDisplay> getAllTemplates();

    S3ImportTemplateDisplay getTemplate(String feedType, String templateName);

    SourceFile uploadFile(String filePath, EntityType entity);

    SourceFile uploadDeleteFile(String csvFileName, String schemaInterpretation, String cleanupOperationType,
            Resource source);

    FieldMappingDocument getFieldMappings(String sourceFileName, EntityType entity, SourceType source, String feedType);

    void saveFieldMappingDocument(String displayName, FieldMappingDocument fieldMappingDocument, EntityType entity,
            SourceType source, String feedType);

    void createS3Template(String templateFileName, SourceType source, boolean importData,
            S3ImportTemplateDisplay templateDisplay);

    void upsertDefaultTemplateByAutoMapping(String filePath, EntityType entity, boolean importData);

    void upsertTemplateByAutoMapping(String filePath, String templateName, String feedType, EntityType entity,
            boolean importData, S3ImportTemplateDisplay template);

    void doDefaultTemplateOneOffImport(String filePath, EntityType entity);

    void doOneOffImport(String filePath, String systemName, EntityType entity);
}
