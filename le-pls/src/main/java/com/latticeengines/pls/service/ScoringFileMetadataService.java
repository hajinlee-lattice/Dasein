package com.latticeengines.pls.service;

import java.io.InputStream;
import java.util.List;
import java.util.Set;

import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.pls.frontend.ScoringFieldMappingDocument;

public interface ScoringFileMetadataService {

    InputStream validateHeaderFields(InputStream stream, List<Attribute> requiredColumns,
            CloseableResourcePool pool, String fileName);

    Table registerMetadataTable(SourceFile sourceFile, String modelId);

    FieldMappingDocument mapRequiredFieldsWithFileHeaders(String csvFileName,
            String modelId);

    void saveFieldMappingDocument(String csvFileName, String modelId, FieldMappingDocument fieldMappingDocument);

    Set<String> getHeaderFields(String csvFileName);

}
