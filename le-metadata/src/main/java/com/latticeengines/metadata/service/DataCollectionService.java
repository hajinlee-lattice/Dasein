package com.latticeengines.metadata.service;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.DataCollection;

public interface DataCollectionService {

    List<DataCollection> getDataCollections(String customerSpace);

    DataCollection getDefaultDataCollection(String customerSpace);

    DataCollection createDefaultDataCollection(String customerSpace, String statisticsId, List<String> tableNames);

    DataCollection getDataCollection(String customerSpace, String dataCollectionName);

    DataCollection createDataCollection(String customerSpace, String statisticsId, List<String> tableNames);
}
