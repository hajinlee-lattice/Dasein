package com.latticeengines.metadata.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.metadata.DataFeed;
import com.latticeengines.domain.exposed.metadata.DataFeedExecution;

public interface DataFeedEntityMgr extends BaseEntityMgr<DataFeed> {

    DataFeedExecution startExecution(String datafeedName);

    DataFeed findByName(String datafeedName);

    DataFeedExecution finishExecution(String datafeedName);

}
