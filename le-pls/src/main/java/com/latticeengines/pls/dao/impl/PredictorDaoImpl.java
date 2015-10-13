package com.latticeengines.pls.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.pls.dao.PredictorDao;

@Component("predictorDao")
public class PredictorDaoImpl extends BaseDaoImpl<Predictor> implements PredictorDao {

    @Override
    protected Class<Predictor> getEntityClass() {
        return Predictor.class;
    }

    @Override
    public List<Predictor> findByModelId(String modelId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<Predictor> entityClz = getEntityClass();
        String queryStr = String.format("from %s where modelSummary. = '%s'", entityClz.getSimpleName(), modelId);
        Query query = session.createQuery(queryStr);
        return query.list();
    }

}
