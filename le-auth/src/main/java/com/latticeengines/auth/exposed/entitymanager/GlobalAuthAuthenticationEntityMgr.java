package com.latticeengines.auth.exposed.entitymanager;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.auth.GlobalAuthAuthentication;

public interface GlobalAuthAuthenticationEntityMgr extends BaseEntityMgr<GlobalAuthAuthentication> {

    GlobalAuthAuthentication findByUsername(String username);

    GlobalAuthAuthentication findByUsernameJoinUser(String username);
}
