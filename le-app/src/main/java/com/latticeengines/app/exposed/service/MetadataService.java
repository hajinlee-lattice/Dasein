package com.latticeengines.app.exposed.service;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;

// This is used for saving/retrieving attributes in lattice insights with customizations for UI purposes
public interface MetadataService {
    List<ColumnMetadata> getAttributes(Integer offset, Integer max);

    Statistics getStatistics();
}
