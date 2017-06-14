package com.latticeengines.network.exposed.metadata;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;

public interface DataCollectionInterface {
    List<DataCollection> getDataCollections(String customerSpace);

    DataCollection getDataCollectionByType(String customerSpace, DataCollectionType type);

    DataCollection createOrUpdateDataCollection(String customerSpace, DataCollection dataCollection);

    DataCollection upsertTableToDataCollection(String customerSpace, //
                                               DataCollectionType dataCollectionType, //
                                               String tableName, //
                                               boolean purgeOldTable);

    DataCollection upsertStatsToDataCollection(String customerSpace, DataCollectionType dataCollectionType,
                                               StatisticsContainer container, boolean purgeOld);
}
