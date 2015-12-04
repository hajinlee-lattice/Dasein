package com.latticeengines.pls.entitymanager;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.TenantDeployment;

public interface TenantDeploymentEntityMgr extends BaseEntityMgr<TenantDeployment> {

    TenantDeployment findByTenantId(Long tenantId);

    boolean deleteByTenantId(Long tenantId);
}
