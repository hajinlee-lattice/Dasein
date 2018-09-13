package com.latticeengines.modelquality.dao.impl;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.modelquality.Algorithm;
import com.latticeengines.modelquality.dao.AlgorithmDao;

@Component("qualityAlgorithmDao")
public class AlgorithmDaoImpl extends ModelQualityBaseDaoImpl<Algorithm> implements AlgorithmDao {

    @Override
    protected Class<Algorithm> getEntityClass() {
        return Algorithm.class;
    }

    @Override
    public Algorithm findByMaxVersion() {
        Session session = getSessionFactory().getCurrentSession();
        String queryStr = String.format("from %s where version = (select MAX(version) from %s)",
                getEntityClass().getSimpleName(), getEntityClass().getSimpleName());
        Query<Algorithm> query = session.createQuery(queryStr, Algorithm.class);
        List<Algorithm> results = query.list();
        if (results.size() == 0) {
            return null;
        }
        if (results.size() > 1) {
            throw new RuntimeException("Multiple rows found with the same value for VERSION");
        }
        return results.get(0);
    }
}
