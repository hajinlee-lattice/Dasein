package com.latticeengines.auth.exposed.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.auth.GlobalAuthUser;

public interface GlobalAuthUserEntityMgr extends BaseEntityMgr<GlobalAuthUser> {

    GlobalAuthUser findByUserId(Long userId);

    GlobalAuthUser findByUserIdWithTenantRightsAndAuthentications(Long userId);

    GlobalAuthUser findByEmailJoinAuthentication(String email);

    List<GlobalAuthUser> findByEmailJoinUserTenantRight(String email);

    GlobalAuthUser findByEmail(String email);

}
