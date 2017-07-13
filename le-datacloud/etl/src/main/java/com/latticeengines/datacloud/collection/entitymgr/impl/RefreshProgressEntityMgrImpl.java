package com.latticeengines.datacloud.collection.entitymgr.impl;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.datacloud.collection.dao.RefreshProgressDao;
import com.latticeengines.datacloud.collection.entitymgr.RefreshProgressEntityMgr;
import com.latticeengines.datacloud.core.source.DerivedSource;
import com.latticeengines.domain.exposed.datacloud.manage.RefreshProgress;

@Component("refreshProgressEntityMgr")
public class RefreshProgressEntityMgrImpl
        extends AbstractProgressEntityMgr<RefreshProgress> implements RefreshProgressEntityMgr {

    private Logger log = LoggerFactory.getLogger(this.getClass());

    @Autowired
    RefreshProgressDao progressDao;

    @Override
    protected RefreshProgressDao getProgressDao() { return progressDao; }

    @Override
    protected Logger getLog() { return log; }

    @Override
    @Transactional(value = "propDataManage")
    public RefreshProgress insertNewProgress(DerivedSource source, Date pivotDate, String creator) {
        try {
            RefreshProgress newProgress = RefreshProgress.constructByDate(source.getSourceName(), pivotDate);
            newProgress.setCreatedBy(creator);
            progressDao.create(newProgress);
            return newProgress;
        } catch (IllegalAccessException|InstantiationException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @Transactional(value = "propDataManage", readOnly = true)
    public RefreshProgress findProgressByBaseVersion(DerivedSource source, String baseVersion) {
        return progressDao.findByBaseSourceVersion(source, baseVersion);
    }


}
