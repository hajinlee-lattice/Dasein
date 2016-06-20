package com.latticeengines.security.entitymanager;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.security.GlobalAuthAuthentication;

public interface GlobalAuthAuthenticationEntityMgr extends BaseEntityMgr<GlobalAuthAuthentication> {

    GlobalAuthAuthentication findByUsername(String username);

    GlobalAuthAuthentication findByUsernameJoinUser(String username);
}
