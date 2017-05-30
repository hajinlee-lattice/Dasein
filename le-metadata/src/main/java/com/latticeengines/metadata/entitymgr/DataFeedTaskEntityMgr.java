package com.latticeengines.metadata.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.metadata.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.Table;

public interface DataFeedTaskEntityMgr extends BaseEntityMgr<DataFeedTask> {

    Table pollFirstDataTable(Long taskPid);

    Table peekFirstDataTable(Long taskPid);

    int getDataTableSize(Long taskPid);

}
