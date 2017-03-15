package com.latticeengines.modelquality.entitymgr.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.modelquality.AnalyticTest;
import com.latticeengines.domain.exposed.modelquality.ModelRun;
import com.latticeengines.modelquality.dao.ModelRunDao;
import com.latticeengines.modelquality.entitymgr.ModelRunEntityMgr;

@Component("qualityModelRunEntityMgr")
public class ModelRunEntityMgrImpl extends BaseEntityMgrImpl<ModelRun> implements ModelRunEntityMgr {

    @Autowired
    private ModelRunDao modelRunDao;

    @Override
    public BaseDao<ModelRun> getDao() {
        return modelRunDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(ModelRun modelRun) {
        modelRun.setName(modelRun.getName().replace('/', '_'));
        modelRunDao.create(modelRun);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public ModelRun findByName(String modelRunName) {
        return modelRunDao.findByField("NAME", modelRunName);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<ModelRun> findModelRunsByAnalyticTest(AnalyticTest analyticTest) {
        return modelRunDao.findAllByAnalyticTest(analyticTest);
    }

}
