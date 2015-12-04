package com.latticeengines.pls.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.TenantDeployment;

public interface TenantDeploymentDao extends BaseDao<TenantDeployment> {

    TenantDeployment findByTenantId(long tenantId);

    boolean deleteByTenantId(long tenantId);
}
