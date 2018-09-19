package com.latticeengines.apps.lp.dao.impl;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.lp.dao.PredictorDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.Predictor;

/**
 * Change to this dao should also be made to le-pls
 */
@Component("predictorDao")
public class PredictorDaoImpl extends BaseDaoImpl<Predictor> implements PredictorDao {

    @Override
    protected Class<Predictor> getEntityClass() {
        return Predictor.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<Predictor> findByModelId(String modelId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<Predictor> entityClz = getEntityClass();
        String queryStr = String.format("from %s where sourceModelSummary. = '%s'", entityClz.getSimpleName(), modelId);
        Query<Predictor> query = session.createQuery(queryStr);
        return query.list();
    }

}
