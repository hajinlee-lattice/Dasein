package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;

public interface DataCollectionService {

    DataCollection getDataCollection(String customerSpace, String collectionName);

    DataCollection.Version getActiveVersion(String customerSpace);

    DataCollection.Version switchDataCollectionVersion(String customerSpace, String collectionName,
                                                       DataCollection.Version version);

    DataCollection getOrCreateDefaultCollection(String customerSpace);

    void addStats(String customerSpace, String collectionName, StatisticsContainer container);

    void removeStats(String customerSpace, String collectionName, DataCollection.Version version);

    void upsertTable(String customerSpace, String collectionName, String tableName, TableRoleInCollection tableRole,
                     DataCollection.Version version);

    void upsertTables(String customerSpace, String collectionName, String[] tableNames, TableRoleInCollection tableRole,
            DataCollection.Version version);

    void removeTable(String customerSpace, String collectionName, String tableName, TableRoleInCollection tableRole,
                     DataCollection.Version version);

    List<Table> getTables(String customerSpace, String collectionName, TableRoleInCollection tableRole,
                          DataCollection.Version version);

    List<String> getTableNames(String customerSpace, String collectionName, TableRoleInCollection tableRole,
                               DataCollection.Version version);

    StatisticsContainer getStats(String customerSpace, String collectionName, DataCollection.Version version);

    AttributeRepository getAttrRepo(String customerSpace, String collectionName, DataCollection.Version version);

    void resetTable(String customerSpace, String collectionName, TableRoleInCollection tableRole);

    String updateDataCloudBuildNumber(String customerSpace, String collectionName, String dataCloudBuildNumber);

    void clearCache(String customerSpace);

}
