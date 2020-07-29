package com.latticeengines.pls.service;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.pls.frontend.FieldValidationResult;
import com.latticeengines.domain.exposed.pls.frontend.LatticeSchemaField;

public interface ModelingFileMetadataService {

    FieldMappingDocument getFieldMappingDocumentBestEffort(String sourceFileName,
            SchemaInterpretation schemaInterpretation, ModelingParameters parameters, boolean isModel,
                                                           boolean withoutId, boolean enableEntityMatch,
                                                           boolean onlyGA);

    FieldMappingDocument getFieldMappingDocumentBestEffort(String sourceFileName,
            String entity, String source, String feedType);

    void resolveMetadata(String sourceFileName, FieldMappingDocument fieldMappingDocument, boolean isModel,
            boolean enableEntityMatch, boolean onlyGA);
    void resolveMetadata(String sourceFileName, FieldMappingDocument fieldMappingDocument,
                         String entity, String source, String feedType);

    InputStream validateHeaderFields(InputStream stream, CloseableResourcePool leCsvParser,
            String fileName, boolean checkHeaderFormat);

    InputStream validateHeaderFields(InputStream stream, CloseableResourcePool leCsvParser,
                                     String fileName, boolean checkHeaderFormat, String entity);

    Map<SchemaInterpretation, List<LatticeSchemaField>> getSchemaToLatticeSchemaFields(
            boolean excludeLatticeDataAttributes);

    List<LatticeSchemaField> getSchemaToLatticeSchemaFields(SchemaInterpretation schemaInterpretation);

    List<LatticeSchemaField> getSchemaToLatticeSchemaFields(String entity, String source, String feedType);

    FieldValidationResult validateFieldMappings(String sourceFileName, FieldMappingDocument fieldMappingDocument,
                                                String entity, String source, String feedType);

}
