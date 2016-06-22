package com.latticeengines.security.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserTenantRight;

public interface GlobalAuthUserTenantRightDao extends BaseDao<GlobalAuthUserTenantRight> {

    List<GlobalAuthUserTenantRight> findByUserIdAndTenantId(Long userId, Long tenantId);

    GlobalAuthUserTenantRight findByUserIdAndTenantIdAndOperationName(Long userId, Long tenantId,
            String operationName);
}
