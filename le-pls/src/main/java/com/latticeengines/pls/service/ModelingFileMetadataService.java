package com.latticeengines.pls.service;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.pls.frontend.LatticeSchemaField;

public interface ModelingFileMetadataService {

    FieldMappingDocument getFieldMappingDocumentBestEffort(String sourceFileName,
            SchemaInterpretation schemaInterpretation, ModelingParameters parameters);

    void resolveMetadata(String sourceFileName, FieldMappingDocument fieldMappingDocument);

    InputStream validateHeaderFields(InputStream stream, CloseableResourcePool leCsvParser,
            String fileName);

    Map<SchemaInterpretation, List<LatticeSchemaField>> getSchemaToLatticeSchemaFields(
            boolean excludeLatticeDataAttributes);

    List<LatticeSchemaField> getSchemaToLatticeSchemaFields(SchemaInterpretation schemaInterpretation);
}
