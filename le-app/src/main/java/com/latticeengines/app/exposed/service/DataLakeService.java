package com.latticeengines.app.exposed.service;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public interface DataLakeService {

    List<ColumnMetadata> getAttributes(Integer start, Integer limit);
    List<ColumnMetadata> getAttributesInEntity(BusinessEntity entity);
    Statistics getStatistics();

    Statistics getDemoStatistics();
    List<ColumnMetadata> getDemoAttributes(BusinessEntity entity);

}
