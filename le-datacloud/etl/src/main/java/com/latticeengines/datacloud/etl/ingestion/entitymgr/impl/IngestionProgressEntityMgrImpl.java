package com.latticeengines.datacloud.etl.ingestion.entitymgr.impl;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.datacloud.etl.ingestion.dao.IngestionProgressDao;
import com.latticeengines.datacloud.etl.ingestion.entitymgr.IngestionProgressEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;

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
    public List<IngestionProgress> getProgressesByField(Map<String, Object> fields, List<String> orderFields) {
        return ingestionProgressDao.getProgressesByField(fields, orderFields);
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
