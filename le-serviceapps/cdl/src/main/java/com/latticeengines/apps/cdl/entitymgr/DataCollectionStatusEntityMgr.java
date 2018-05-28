package com.latticeengines.apps.cdl.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.security.Tenant;

public interface DataCollectionStatusEntityMgr extends BaseEntityMgrRepository<DataCollectionStatus, Long> {

    DataCollectionStatus findByTenantAndVersion(Tenant tenant, DataCollection.Version version);

}
