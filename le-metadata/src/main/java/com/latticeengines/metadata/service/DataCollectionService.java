package com.latticeengines.metadata.service;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;

public interface DataCollectionService {

    List<DataCollection> getDataCollections(String customerSpace);

    DataCollection getDataCollectionByType(String customerSpace, DataCollectionType type);

    DataCollection getDataCollection(String customerSpace, String dataCollectionName);

    DataCollection createDataCollection(String customerSpace, DataCollection dataCollection);
}
