package com.latticeengines.auth.exposed.dao;

import java.util.HashMap;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;
import com.latticeengines.domain.exposed.auth.GlobalAuthUser;

public interface GlobalAuthUserDao extends BaseDao<GlobalAuthUser> {

    GlobalAuthUser findByUserIdWithTenantRightsAndAuthentications(Long userId);

    GlobalAuthUser findByEmailJoinAuthentication(String email);

    HashMap<Long, String> findUserInfoByTenant(GlobalAuthTenant tenant);

}
