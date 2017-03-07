package com.latticeengines.metadata.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;

public interface DataCollectionEntityMgr extends BaseEntityMgr<DataCollection> {
    DataCollection createDataCollection(DataCollection dataCollection);

    DataCollection getDataCollection(String name);

    void removeDataCollection(String name);

    DataCollection getDataCollection(DataCollectionType type);
}
