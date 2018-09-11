package com.latticeengines.auth.exposed.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.auth.GlobalAuthAuthentication;

public interface GlobalAuthAuthenticationDao extends BaseDao<GlobalAuthAuthentication> {

    GlobalAuthAuthentication findByUsernameJoinUser(String username);

}
