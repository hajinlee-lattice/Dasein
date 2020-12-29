package com.latticeengines.apps.cdl.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.metadata.DataOperation;

public interface DataOperationEntityMgr extends BaseEntityMgrRepository<DataOperation, Long> {

    DataOperation findByDropPath(String dropPath);
}
