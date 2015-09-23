package com.latticeengines.pls.dao.impl;

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

}
