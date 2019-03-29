package com.latticeengines.auth.exposed.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserTenantRight;

public interface GlobalAuthUserTenantRightEntityMgr extends
        BaseEntityMgr<GlobalAuthUserTenantRight> {

    List<GlobalAuthUserTenantRight> findByUserIdAndTenantId(Long userId, Long tenantId);

    List<GlobalAuthUser> findUsersByTenantId(Long tenantId);

    GlobalAuthUserTenantRight findByUserIdAndTenantIdAndOperationName(Long userId, Long tenantId,
            String operationName);

    List<GlobalAuthUserTenantRight> findByEmail(String email);

    List<GlobalAuthUserTenantRight> findByTenantId(Long tenantId);

    Boolean deleteByUserId(Long userId);

    boolean isRedundant(String email);

    List<GlobalAuthUserTenantRight> findByNonNullExprationDate();

}
