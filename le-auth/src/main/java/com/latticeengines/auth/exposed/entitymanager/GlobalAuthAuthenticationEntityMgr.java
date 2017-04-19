package com.latticeengines.auth.exposed.entitymanager;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.auth.GlobalAuthAuthentication;

import java.util.HashMap;

public interface GlobalAuthAuthenticationEntityMgr extends BaseEntityMgr<GlobalAuthAuthentication> {

    GlobalAuthAuthentication findByUsername(String username);

    GlobalAuthAuthentication findByUsernameJoinUser(String username);

    GlobalAuthAuthentication findByUserId(Long userId);

    HashMap<Long, String> findUserInfoByTenantId(Long tenantId);

}
