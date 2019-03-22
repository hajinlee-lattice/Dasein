package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatusHistory;

public interface DataCollectionStatusHistoryEntityMgr
        extends BaseEntityMgrRepository<DataCollectionStatusHistory, Long> {

    List<DataCollectionStatusHistory> findByTenantNameOrderByCreationTimeDesc(String tenantName);

}
