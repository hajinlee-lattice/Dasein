package com.latticeengines.datacloud.etl.transformation.dao.impl;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.etl.transformation.dao.LatticeIdStrategyDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.datacloud.manage.LatticeIdStrategy;

@Component("latticeIdStrategyDao")
public class LatticeIdStrategyDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<LatticeIdStrategy>
        implements LatticeIdStrategyDao {
    @Override
    protected Class<LatticeIdStrategy> getEntityClass() {
        return LatticeIdStrategy.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<LatticeIdStrategy> getStrategyByName(String name) {
        Session session = getSessionFactory().getCurrentSession();
        Class<LatticeIdStrategy> entityClz = getEntityClass();
        String queryStr = String.format("from %s where Strategy = :strategy", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("strategy", name);
        List<LatticeIdStrategy> resultList = query.list();
        if (CollectionUtils.isEmpty(resultList)) {
            return null;
        } else {
            return resultList;
        }
    }
}
