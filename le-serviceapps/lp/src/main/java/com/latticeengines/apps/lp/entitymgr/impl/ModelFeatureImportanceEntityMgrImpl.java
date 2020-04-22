package com.latticeengines.apps.lp.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.lp.dao.ModelFeatureImportanceDao;
import com.latticeengines.apps.lp.entitymgr.ModelFeatureImportanceEntityMgr;
import com.latticeengines.apps.lp.repository.writer.ModelFeatureImportanceWriterRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.ModelFeatureImportance;

@Component("modelFeatureImportanceEntityMgr")
public class ModelFeatureImportanceEntityMgrImpl extends BaseEntityMgrRepositoryImpl<ModelFeatureImportance, Long>
        implements ModelFeatureImportanceEntityMgr {

    @Inject
    private ModelFeatureImportanceWriterRepository repository;

    @Inject
    private ModelFeatureImportanceDao dao;

    @Override
    public BaseJpaRepository<ModelFeatureImportance, Long> getRepository() {
        return repository;
    }

    @Override
    public BaseDao<ModelFeatureImportance> getDao() {
        return dao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<ModelFeatureImportance> getByModelGuid(String modelGuid) {
        return repository.findByModelSummary_Id(modelGuid);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void createFeatureImportances(List<ModelFeatureImportance> importances) {
        dao.create(importances);
    }

}
