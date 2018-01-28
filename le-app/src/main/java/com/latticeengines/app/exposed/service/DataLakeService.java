package com.latticeengines.app.exposed.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;

public interface DataLakeService {

    List<ColumnMetadata> getAttributes(Integer start, Integer limit);

    long getAttributesCount();

    TopNTree getTopNTree();

    Map<String, StatsCube> getStatsCubes();

    Map<String, StatsCube> getStatsCubes(String customerSpace);

    AttributeStats getAttributeStats(BusinessEntity entity, String attribute);

    List<ColumnMetadata> getAttributesInTableRole(String customerSpace, TableRoleInCollection role);

    List<ColumnMetadata> getAttributesInPredefinedGroup(Predefined predefined);

    DataPage getAccountById(String accountID, Predefined predefined);

}
