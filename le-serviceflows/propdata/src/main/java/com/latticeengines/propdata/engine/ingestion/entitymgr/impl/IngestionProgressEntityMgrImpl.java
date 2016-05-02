package com.latticeengines.propdata.engine.ingestion.entitymgr.impl;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.propdata.manage.Ingestion;
import com.latticeengines.domain.exposed.propdata.manage.IngestionProgress;
import com.latticeengines.propdata.engine.ingestion.dao.IngestionProgressDao;
import com.latticeengines.propdata.engine.ingestion.entitymgr.IngestionProgressEntityMgr;

@Component("ingestionProgressEntityMgr")
public class IngestionProgressEntityMgrImpl implements IngestionProgressEntityMgr {
    @Autowired
    private IngestionProgressDao ingestionProgressDao;

    @Override
    @Transactional(value = "propDataManage", readOnly = true)
    public IngestionProgress getProgress(IngestionProgress progress) {
        return ingestionProgressDao.findByKey(progress);
    }

    @Override
    @Transactional(value = "propDataManage", readOnly = true)
    public List<IngestionProgress> getProgressesByField(Map<String, Object> fields) {
        return ingestionProgressDao.getProgressesByField(fields);
    }

    @Override
    @Transactional(value = "propDataManage")
    public IngestionProgress saveProgress(IngestionProgress progress) {
        return ingestionProgressDao.saveProgress(progress);
    }

    @Override
    @Transactional(value = "propDataManage")
    public void deleteProgress(IngestionProgress progress) {
        ingestionProgressDao.delete(progress);
    }

    @Override
    @Transactional(value = "propDataManage")
    public void deleteProgressByField(Map<String, Object> fields) {
        ingestionProgressDao.deleteProgressByField(fields);
    }

    @Override
    @Transactional(value = "propDataManage", readOnly = true)
    public boolean isIngestionTriggered(Ingestion ingestion) {
        return ingestionProgressDao.isIngestionTriggered(ingestion);
    }

    @Override
    @Transactional(value = "propDataManage", readOnly = true)
    public List<IngestionProgress> getRetryFailedProgresses() {
        return ingestionProgressDao.getRetryFailedProgresses();
    }

    @Override
    @Transactional(value = "propDataManage", readOnly = true)
    public boolean isDuplicateProgress(IngestionProgress progress) {
        return ingestionProgressDao.isDuplicateProgress(progress);
    }
}
