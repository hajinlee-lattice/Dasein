package com.latticeengines.app.exposed.service;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;

@Deprecated
public interface MetadataService {
    List<ColumnMetadata> getAttributes(Integer offset, Integer max);

    Statistics getStatistics();
}
