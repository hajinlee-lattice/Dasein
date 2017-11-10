package com.latticeengines.apps.cdl.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.dataloader.DLTenantMapping;

public interface DLTenantMappingEntityMgr extends BaseEntityMgr<DLTenantMapping> {

    DLTenantMapping getDLTenantMapping(String dlTenantId, String dlLoadGroup);

    DLTenantMapping getDLTenantMapping(String dlTenantId);
}
