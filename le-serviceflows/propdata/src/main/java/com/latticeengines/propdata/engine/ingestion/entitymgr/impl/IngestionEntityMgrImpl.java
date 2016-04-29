package com.latticeengines.propdata.engine.ingestion.entitymgr.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.propdata.manage.Ingestion;
import com.latticeengines.propdata.engine.ingestion.dao.IngestionDao;
import com.latticeengines.propdata.engine.ingestion.entitymgr.IngestionEntityMgr;

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
}
