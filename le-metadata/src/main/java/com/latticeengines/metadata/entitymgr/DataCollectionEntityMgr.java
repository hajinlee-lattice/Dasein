package com.latticeengines.metadata.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.metadata.DataCollection;

public interface DataCollectionEntityMgr extends BaseEntityMgr<DataCollection> {
    DataCollection createDataCollection(List<String> tableNames, String statisticsId, boolean isDefault);

    DataCollection getDefaultDataCollection();

    DataCollection getDataCollection(String name);

    void removeDataCollection(String name);
}
