package com.latticeengines.network.exposed.metadata;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.DataCollection;

public interface DataCollectionInterface {
    List<DataCollection> getDataCollections(String customerSpace);

    DataCollection getDefaultDataCollection(String customerSpace);

    DataCollection createDefaultDataCollection(String customerSpace, String statisticsId, List<String> tableNames);

    DataCollection getDataCollection(String customerSpace, String DataCollectionName);

    DataCollection createDataCollection(String customerSpace, String statisticsId, List<String> tableNames);
}
