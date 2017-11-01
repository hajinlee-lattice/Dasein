package com.latticeengines.app.exposed.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public interface DataLakeService {

    List<ColumnMetadata> getAttributes(Integer start, Integer limit);

    List<ColumnMetadata> getAttributesInPredefinedGroup(ColumnSelection.Predefined predefined);

    long getAttributesCount();

    TopNTree getTopNTree(boolean includeTopBkt);

    StatsCube getStatsCube();

    Map<BusinessEntity, StatsCube> getStatsCubes();

    AttributeStats getAttributeStats(BusinessEntity entity, String attribute);

}
