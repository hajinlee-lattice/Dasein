package com.latticeengines.auth.exposed.entitymanager;

import java.util.HashMap;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;
import com.latticeengines.domain.exposed.auth.GlobalAuthUser;

public interface GlobalAuthUserEntityMgr extends BaseEntityMgr<GlobalAuthUser> {

    GlobalAuthUser findByUserId(Long userId);

    GlobalAuthUser findByUserIdWithTenantRightsAndAuthentications(Long userId);

    GlobalAuthUser findByEmailJoinAuthentication(String email);

    GlobalAuthUser findByEmail(String email);

    HashMap<Long, String> findUserInfoByTenant(GlobalAuthTenant tenant);

}
