package com.latticeengines.apps.cdl.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.cdl.ConvertBatchStoreInfo;

public interface ConvertBatchStoreInfoEntityMgr extends BaseEntityMgrRepository<ConvertBatchStoreInfo, Long> {

    ConvertBatchStoreInfo findByPid(Long pid);
}
