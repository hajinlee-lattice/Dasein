package com.latticeengines.pls.service;

import java.io.InputStream;
import java.util.List;

import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SourceFile;

public interface ScoringFileMetadataService {

    InputStream validateHeaderFields(InputStream stream, List<Attribute> requiredColumns, CloseableResourcePool pool,
            String fileName);

    Table registerMetadataTable(SourceFile sourceFile, String modelId);
}
