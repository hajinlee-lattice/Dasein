package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.metadata.DataCollection.Version;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatusHistory;
import com.latticeengines.domain.exposed.security.Tenant;

public interface DataCollectionStatusHistoryEntityMgr
        extends BaseEntityMgrRepository<DataCollectionStatusHistory, Long> {

    List<DataCollectionStatusHistory> findByTenantAndVersionOrderByCreatedDesc(Tenant tenant, Version version);

}
