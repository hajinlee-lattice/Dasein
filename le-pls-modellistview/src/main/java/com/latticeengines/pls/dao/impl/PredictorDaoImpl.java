package com.latticeengines.pls.dao.impl;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.pls.dao.PredictorDao;


public class PredictorDaoImpl extends BaseDaoImpl<Predictor> implements PredictorDao {

    @Override
    protected Class<Predictor> getEntityClass() {
        return Predictor.class;
    }

}
