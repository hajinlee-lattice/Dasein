package com.latticeengines.pls.service;

import java.io.InputStream;
import java.util.List;

import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.domain.exposed.metadata.Attribute;

public interface ScoringFileMetadataService {

    InputStream validateHeaderFields(InputStream stream, List<String> requiredColumns, CloseableResourcePool pool,
            String fileName);
}
