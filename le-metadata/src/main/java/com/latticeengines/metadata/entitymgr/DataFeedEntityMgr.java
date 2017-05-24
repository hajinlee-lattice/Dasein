package com.latticeengines.metadata.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.metadata.DataFeed;

public interface DataFeedEntityMgr extends BaseEntityMgr<DataFeed> {

    void startExecution(String datafeedName);

}
