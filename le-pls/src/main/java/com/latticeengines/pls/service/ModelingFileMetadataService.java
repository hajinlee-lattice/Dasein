package com.latticeengines.pls.service;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.pls.frontend.LatticeSchemaField;
import com.latticeengines.pls.metadata.resolution.ColumnTypeMapping;

public interface ModelingFileMetadataService {

    FieldMappingDocument mapFieldDocumentBestEffort(String sourceFileName);

    void resolveMetadata(String sourceFileName, FieldMappingDocument fieldMappingDocument);

    List<ColumnTypeMapping> getUnknownColumns(String sourceFileName);

    void resolveMetadata(String sourceFileName, List<ColumnTypeMapping> unknownColumns);

    InputStream validateHeaderFields(InputStream stream, CloseableResourcePool leCsvParser, String fileName);

    InputStream validateHeaderFields(InputStream stream, SchemaInterpretation schema, CloseableResourcePool leCsvParser, String fileName);

    Map<SchemaInterpretation, List<LatticeSchemaField>> getSchemaToLatticeSchemaFields();
}
