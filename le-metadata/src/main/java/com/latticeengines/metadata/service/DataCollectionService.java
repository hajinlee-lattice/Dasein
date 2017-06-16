package com.latticeengines.metadata.service;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;

public interface DataCollectionService {

    List<DataCollection> getDataCollections(String customerSpace);

    DataCollection getDataCollection(String customerSpace, String collectionName);

    DataCollection getDataCollectionByType(String customerSpace, DataCollectionType type);

    DataCollection createOrUpdateDataCollection(String customerSpace, DataCollection dataCollection);

    void upsertStats(String customerSpace, String collectionName, StatisticsContainer container,
            String modelId);

    void upsertTable(String customerSpace, String collectionName, String tableName,
            TableRoleInCollection tableRole);

    List<Table> getTables(String customerSpace, String collectionName, TableRoleInCollection tableRole);

    StatisticsContainer getStats(String customerSpace, String collectionName, String modelId);

}
