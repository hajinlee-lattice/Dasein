package com.latticeengines.pls.service;

import java.io.InputStream;
import java.util.List;

import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.pls.metadata.resolution.ColumnTypeMapping;

public interface ModelingFileMetadataService {

    List<ColumnTypeMapping> getUnknownColumns(String sourceFileName);

    void resolveMetadata(String sourceFileName, List<ColumnTypeMapping> unknownColumns);

    InputStream validateHeaderFields(InputStream stream, SchemaInterpretation schema, CloseableResourcePool leCsvParser, String fileName);
}
