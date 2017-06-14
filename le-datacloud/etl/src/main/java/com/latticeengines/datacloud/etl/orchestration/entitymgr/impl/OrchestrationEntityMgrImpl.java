package com.latticeengines.datacloud.etl.orchestration.entitymgr.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.datacloud.etl.orchestration.dao.OrchestrationDao;
import com.latticeengines.datacloud.etl.orchestration.entitymgr.OrchestrationEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.Orchestration;

@Component("orchestrationEntityMgr")
public class OrchestrationEntityMgrImpl implements OrchestrationEntityMgr {

    @Autowired
    private OrchestrationDao orchestrationDao;

    @Override
    @Transactional(value = "propDataManage", readOnly = true)
    public List<Orchestration> findAll() {
        return orchestrationDao.findAll();
    }

    @Override
    @Transactional(value = "propDataManage", readOnly = true)
    public Orchestration findByField(String fieldName, Object value) {
        return orchestrationDao.findByField(fieldName, value);
    }

    @Override
    @Transactional(value = "propDataManage")
    public void save(Orchestration orch) {
        orchestrationDao.createOrUpdate(orch);
    }

    @Override
    @Transactional(value = "propDataManage")
    public void delete(Orchestration orch) {
        orchestrationDao.delete(orch);
    }




}
