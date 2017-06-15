package com.latticeengines.metadata.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;

public interface DataCollectionEntityMgr extends BaseEntityMgr<DataCollection> {
    void createDataCollection(DataCollection dataCollection);

    DataCollection getDataCollection(String name);

    void removeDataCollection(String name);

    DataCollection getDataCollection(DataCollectionType type);

    void fillInTables(DataCollection collection);

    List<Table> getTablesOfRole(String collectionName, TableRoleInCollection tableRole);

    void upsertTableToCollection(String collectionName, String tableName, TableRoleInCollection role);

    void removeTableFromCollection(String collectionName, String tableName);

    void upsertStatsForMasterSegment(String collectionName, StatisticsContainer statisticsContainer, String modelId);
}
