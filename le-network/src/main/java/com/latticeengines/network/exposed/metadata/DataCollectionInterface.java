package com.latticeengines.network.exposed.metadata;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;

public interface DataCollectionInterface {
    List<DataCollection> getDataCollections(String customerSpace);

    DataCollection getDataCollectionByType(String customerSpace, DataCollectionType type);

    DataCollection createOrUpdateDataCollection(String customerSpace, DataCollection dataCollection);
}
