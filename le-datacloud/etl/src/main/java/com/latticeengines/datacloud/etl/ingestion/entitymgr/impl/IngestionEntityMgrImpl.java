package com.latticeengines.datacloud.etl.ingestion.entitymgr.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.datacloud.etl.ingestion.dao.IngestionDao;
import com.latticeengines.datacloud.etl.ingestion.entitymgr.IngestionEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;

@Component("ingestionEntityMgr")
public class IngestionEntityMgrImpl implements IngestionEntityMgr {

    @Autowired
    private IngestionDao ingestionDao;

    @Override
    @Transactional(value = "propDataManage", readOnly = true)
    public Ingestion getIngestionByName(String ingestionName) {
        return ingestionDao.getIngestionByName(ingestionName);
    }

    @Override
    @Transactional(value = "propDataManage", readOnly = true)
    public List<Ingestion> findAll() {
        return ingestionDao.findAll();
    }

    @Override
    @Transactional(value = "propDataManage")
    public void save(Ingestion ingestion) {
        ingestionDao.createOrUpdate(ingestion);
    }

    @Override
    @Transactional(value = "propDataManage")
    public void delete(Ingestion ingestion) {
        ingestionDao.delete(ingestion);
    }
}
