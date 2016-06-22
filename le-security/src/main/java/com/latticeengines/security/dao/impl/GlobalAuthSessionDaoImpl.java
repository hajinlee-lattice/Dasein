package com.latticeengines.security.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.auth.GlobalAuthSession;
import com.latticeengines.security.dao.GlobalAuthSessionDao;

@Component("globalAuthSessionDao")
public class GlobalAuthSessionDaoImpl extends BaseDaoImpl<GlobalAuthSession> implements GlobalAuthSessionDao {

    @Override
    protected Class<GlobalAuthSession> getEntityClass() {
        return GlobalAuthSession.class;
    }

}
