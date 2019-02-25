package com.latticeengines.pls.dao.impl;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.SalesforceURL;
import com.latticeengines.pls.dao.SalesforceURLDao;

@Component("salesforceURLDao")
public class SalesforceURLDaoImpl extends BaseDaoImpl<SalesforceURL> implements SalesforceURLDao {

    @Override
    protected Class<SalesforceURL> getEntityClass() {
        return SalesforceURL.class;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public SalesforceURL findByURLName(String urlName) {
        Session session = getSessionFactory().getCurrentSession();
        Class<SalesforceURL> entityClz = getEntityClass();
        String queryStr = String.format("from %s where name = :urlName", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("urlName", urlName);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }

        return (SalesforceURL)list.get(0);
    }
}
