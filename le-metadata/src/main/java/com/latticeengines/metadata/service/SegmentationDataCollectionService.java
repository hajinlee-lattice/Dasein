package com.latticeengines.metadata.service;

import com.latticeengines.domain.exposed.metadata.DataCollection;

public interface SegmentationDataCollectionService {
    DataCollection getDefaultDataCollection();

    void fillInDefaultTables(DataCollection dataCollection);

    void removeDefaultTables(DataCollection dataCollection);
}
