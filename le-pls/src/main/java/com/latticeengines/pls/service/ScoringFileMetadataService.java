package com.latticeengines.pls.service;

import java.io.InputStream;
import java.util.Set;

import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;

public interface ScoringFileMetadataService {

    InputStream validateHeaderFields(InputStream stream, CloseableResourcePool pool,
            String fileName);

    FieldMappingDocument mapRequiredFieldsWithFileHeaders(String csvFileName, String modelId);

    Table saveFieldMappingDocument(String csvFileName, String modelId,
            FieldMappingDocument fieldMappingDocument);

    Set<String> getHeaderFields(String csvFileName);

    void validateHeadersWithDataCloudAttr(Set<String> headers);

}
