package com.latticeengines.pls.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.pls.dao.PlayDao;

@Component("playDao")
public class PlayDaoImpl extends BaseDaoImpl<Play> implements PlayDao {

    @Override
    protected Class<Play> getEntityClass() {
        return Play.class;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Play findByName(String name) {
        Session session = getSessionFactory().getCurrentSession();
        Class<Play> entityClz = getEntityClass();
        String queryStr = String.format("from %s where name = :playName", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("playName", name);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return (Play) list.get(0);
    }

}
