package com.latticeengines.auth.exposed.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.auth.exposed.dao.GlobalAuthSessionDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.auth.GlobalAuthSession;

@Component("globalAuthSessionDao")
public class GlobalAuthSessionDaoImpl extends BaseDaoImpl<GlobalAuthSession> implements GlobalAuthSessionDao {

    @Override
    protected Class<GlobalAuthSession> getEntityClass() {
        return GlobalAuthSession.class;
    }
    
}
