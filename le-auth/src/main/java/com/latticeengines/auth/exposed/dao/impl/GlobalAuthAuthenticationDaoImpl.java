package com.latticeengines.auth.exposed.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.auth.GlobalAuthAuthentication;
import com.latticeengines.auth.exposed.dao.GlobalAuthAuthenticationDao;

@Component("globalAuthAuthenticationDao")
public class GlobalAuthAuthenticationDaoImpl extends BaseDaoImpl<GlobalAuthAuthentication>
        implements
        GlobalAuthAuthenticationDao {

    @Override
    protected Class<GlobalAuthAuthentication> getEntityClass() {
        return GlobalAuthAuthentication.class;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public GlobalAuthAuthentication findByUsernameJoinUser(String username) {
        Session session = sessionFactory.getCurrentSession();
        Class<GlobalAuthAuthentication> entityClz = getEntityClass();
        String queryStr = String.format("from %s where Username = '%s'",
                entityClz.getSimpleName(),
                username);
        Query query = session.createQuery(queryStr);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        } else {
            for (Object auth : list) {
                GlobalAuthAuthentication gaAuth = (GlobalAuthAuthentication) auth;
                if (gaAuth.getGlobalAuthUser() != null) {
                    return gaAuth;
                }
            }
            return (GlobalAuthAuthentication) list.get(0);
        }
    }

}
