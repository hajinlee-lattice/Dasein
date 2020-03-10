package com.latticeengines.auth.exposed.entitymanager;

import java.util.List;
import java.util.Set;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserTenantRight;

public interface GlobalAuthUserTenantRightEntityMgr extends
        BaseEntityMgr<GlobalAuthUserTenantRight> {

    List<GlobalAuthUserTenantRight> findByUserIdAndTenantId(Long userId, Long tenantId);

    List<GlobalAuthUserTenantRight> findByUserIdAndTenantId(Long userId, Long tenantId, boolean inflate);

    List<GlobalAuthUser> findUsersByTenantId(Long tenantId);

    GlobalAuthUserTenantRight findByUserIdAndTenantIdAndOperationName(Long userId, Long tenantId,
                                                                      String operationName);

    List<GlobalAuthUserTenantRight> findByEmail(String email);

    List<GlobalAuthUserTenantRight> findByTenantId(Long tenantId);

    Boolean deleteByUserId(Long userId);

    boolean isRedundant(String email);

    List<GlobalAuthUserTenantRight> findByNonNullExprationDate();

    List<GlobalAuthUserTenantRight> findByEmailsAndTenantId(Set<String> emails, Long tenantId);

    GlobalAuthUserTenantRight findByUserIdAndTenantIdAndOperationName(Long userId,
                                                                      Long tenantId, String operationName, boolean inflate);

}
