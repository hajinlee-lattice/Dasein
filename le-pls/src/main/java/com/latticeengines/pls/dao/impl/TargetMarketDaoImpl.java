package com.latticeengines.pls.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.pls.dao.TargetMarketDao;

@Component("targetMarketDao")
public class TargetMarketDaoImpl extends BaseDaoImpl<TargetMarket> implements TargetMarketDao {

    @Override
    protected Class<TargetMarket> getEntityClass() {
        return TargetMarket.class;
    }

    @Override
    @SuppressWarnings("unchecked")
    public TargetMarket findTargetMarketByName(String name) {
        Session session = getSessionFactory().getCurrentSession();
        Class<TargetMarket> entityClz = getEntityClass();
        String queryStr = String.format("from %s where name = :name", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("name", name);
        List<TargetMarket> targetMarkets = query.list();
        if (targetMarkets.size() == 0) {
            return null;
        }
        return targetMarkets.get(0);
    }

    @Override
    public boolean deleteTargetMarketByName(String name) {
        Session session = getSessionFactory().getCurrentSession();
        Class<TargetMarket> entityClz = getEntityClass();
        String queryStr = String.format("delete from %s where name = :name", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("name", name);
        int numTargetMarketReturned = query.executeUpdate();
        if (numTargetMarketReturned == 1)
            return true;
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public TargetMarket findDefaultTargetMarket() {
        Session session = getSessionFactory().getCurrentSession();
        Class<TargetMarket> entityClz = getEntityClass();
        String queryStr = String.format("from %s where IS_DEFAULT = 1", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        List<TargetMarket> targetMarkets = query.list();
        if (targetMarkets.size() == 0) {
            return null;
        }
        return targetMarkets.get(0);
    }
}
