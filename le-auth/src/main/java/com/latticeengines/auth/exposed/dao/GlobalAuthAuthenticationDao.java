package com.latticeengines.auth.exposed.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.auth.GlobalAuthAuthentication;

import java.util.HashMap;

public interface GlobalAuthAuthenticationDao extends BaseDao<GlobalAuthAuthentication> {

    GlobalAuthAuthentication findByUsernameJoinUser(String username);

    HashMap<Long, String> findUserInfoByTenantId(Long tenantId);

    Boolean deleteByUserId(Long userId) ;

}
