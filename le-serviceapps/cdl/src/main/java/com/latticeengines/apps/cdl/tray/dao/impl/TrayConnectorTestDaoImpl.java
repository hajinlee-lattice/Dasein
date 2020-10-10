package com.latticeengines.apps.cdl.tray.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.tray.dao.TrayConnectorTestDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.cdl.tray.TrayConnectorTest;

@Component("trayConnectorTestDao")
public class TrayConnectorTestDaoImpl extends BaseDaoImpl<TrayConnectorTest> implements TrayConnectorTestDao {

    @Override
    protected Class<TrayConnectorTest> getEntityClass() {
        return TrayConnectorTest.class;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public List<TrayConnectorTest> findUnfinishedTests() {
        Session session = getSessionFactory().getCurrentSession();
        String queryStr = String.format(" FROM %s " //
                + " WHERE TEST_RESULT IS NULL " //
                + " ORDER BY START_TIME ", //
                getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        return query.list();
    }

}
