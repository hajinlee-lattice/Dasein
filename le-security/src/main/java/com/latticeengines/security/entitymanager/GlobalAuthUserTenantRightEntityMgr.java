package com.latticeengines.security.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.security.GlobalAuthUserTenantRight;

public interface GlobalAuthUserTenantRightEntityMgr extends
        BaseEntityMgr<GlobalAuthUserTenantRight> {

    List<GlobalAuthUserTenantRight> findByUserIdAndTenantId(Long userId, Long tenantId);

    GlobalAuthUserTenantRight findByUserIdAndTenantIdAndOperationName(Long userId, Long tenantId,
            String operationName);

}
