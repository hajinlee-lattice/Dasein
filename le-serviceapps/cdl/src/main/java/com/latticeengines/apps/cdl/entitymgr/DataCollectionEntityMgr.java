package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;
import java.util.Map;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionTable;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;

public interface DataCollectionEntityMgr extends BaseEntityMgr<DataCollection> {
    DataCollection createDefaultCollection();

    DataCollection findDefaultCollection();

    DataCollection.Version findActiveVersion();

    DataCollection.Version findInactiveVersion();

    DataCollection getDataCollection(String name);

    List<Table> findTablesOfRole(String collectionName, TableRoleInCollection tableRole, DataCollection.Version version);

    List<String> findTableNamesOfRole(String collectionName, TableRoleInCollection tableRole, DataCollection.Version version);

    List<String> getAllTableName();

    void upsertTableToCollection(String collectionName, String tableName, TableRoleInCollection role,
                                 DataCollection.Version version);

    void removeTableFromCollection(String collectionName, String tableName, DataCollection.Version version);

    void upsertStatsForMasterSegment(String collectionName, StatisticsContainer statisticsContainer);

    List<DataCollectionTable> findTablesFromCollection(String collectionName, String tableName);

    Map<TableRoleInCollection, Map<DataCollection.Version, List<String>>> findTableNamesOfAllRole(String collectionName, TableRoleInCollection tableRole, DataCollection.Version version);

    DataCollectionTable findDataCollectionTableByPid(Long dataCollectionId);

    DataCollectionTable createDataCollectionTable(String collectionName, Table table, TableRoleInCollection role,
                                                  DataCollection.Version version);
}
