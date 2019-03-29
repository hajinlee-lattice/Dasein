package com.latticeengines.auth.exposed.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserTenantRight;

public interface GlobalAuthUserTenantRightDao extends BaseDao<GlobalAuthUserTenantRight> {

    List<GlobalAuthUserTenantRight> findByUserIdAndTenantId(Long userId, Long tenantId);

    List<GlobalAuthUser> findUsersByTenantId(Long tenantId);

    GlobalAuthUserTenantRight findByUserIdAndTenantIdAndOperationName(Long userId, Long tenantId,
            String operationName);

    Boolean deleteByUserId(Long userId);

    List<GlobalAuthUserTenantRight> findByEmail(String email);

    boolean existsByEmail(String email);

    List<GlobalAuthUserTenantRight> findByNonNullExprationDate();
}
