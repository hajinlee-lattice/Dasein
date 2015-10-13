package com.latticeengines.pls.entitymanager.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.pls.dao.PredictorDao;
import com.latticeengines.pls.entitymanager.PredictorEntityMgr;

@Component("predictorEntityMgr")
public class PredictorEntityMgrImpl extends BaseEntityMgrImpl<Predictor> implements PredictorEntityMgr {

    @Autowired
    private PredictorDao predictorDao;

    @Override
    public BaseDao<Predictor> getDao() {
        return predictorDao;
    }

    @Override
    public List<Predictor> findByModelId(String ModelId) {
        return predictorDao.findByModelId(ModelId);
    }

    @Override
    public List<Predictor> findUsedByBuyerInsightsByModelId(String ModelId) {
        // TODO Auto-generated method stub
        return null;
    }

}
