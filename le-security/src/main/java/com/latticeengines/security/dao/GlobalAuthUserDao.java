package com.latticeengines.security.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.auth.GlobalAuthUser;

public interface GlobalAuthUserDao extends BaseDao<GlobalAuthUser> {

    GlobalAuthUser findByUserIdWithTenantRightsAndAuthentications(Long userId);

    GlobalAuthUser findByEmailJoinAuthentication(String email);

    List<GlobalAuthUser> findByEmailJoinUserTenantRight(String email);

    List<GlobalAuthUser> findByTenantIdJoinAuthenticationJoinUserTenantRight(Long tenantId);

}
