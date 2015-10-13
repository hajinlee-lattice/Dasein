package com.latticeengines.pls.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.Predictor;

public interface PredictorDao extends BaseDao<Predictor> {

    List<Predictor> findByModelId(String ModelId);

}
