package com.latticeengines.metadata.service;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;

public interface DataCollectionService {

    List<DataCollection> getDataCollections(String customerSpace);

    DataCollection getDataCollectionByType(String customerSpace, DataCollectionType type);

    DataCollection createOrUpdateDataCollection(String customerSpace, DataCollection dataCollection);

    DataCollection upsertTableToCollection(String customerSpace, DataCollectionType type, String tableName, boolean purgeOldTable);
}
