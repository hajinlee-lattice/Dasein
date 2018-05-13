package com.latticeengines.apps.cdl.entitymgr;

import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.security.Tenant;

public interface DataCollectionStatusEntityMgr {

    DataCollectionStatus findByTenant(Tenant tenant);

    void saveStatus(DataCollectionStatus status);
}
