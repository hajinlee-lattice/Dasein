package com.latticeengines.app.exposed.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;

public interface DataLakeService {

    List<ColumnMetadata> getAttributes(Integer start, Integer limit);

    long getAttributesCount();

    TopNTree getTopNTree();

    TopNTree getTopNTree(String customerSpace);

    Map<String, StatsCube> getStatsCubes();

    Map<String, StatsCube> getStatsCubes(String customerSpace);

    AttributeStats getAttributeStats(BusinessEntity entity, String attribute);

    List<ColumnMetadata> getCachedServingMetadataForEntity(String customerSpace, BusinessEntity entity);

    DataPage getAccountById(String accountID, Predefined predefined, Map<String, String> orgInf);

    DataPage getAccountById(String accountID, Predefined predefined, Map<String, String> orgInf,
            List<String> requiredAttributes);

    DataPage getContactsByAccountById(String accountId, Map<String, String> orgInfo);

}
