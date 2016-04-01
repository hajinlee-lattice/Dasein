package com.latticeengines.propdata.collection.entitymgr.impl;

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.propdata.manage.RefreshProgress;
import com.latticeengines.propdata.collection.dao.RefreshProgressDao;
import com.latticeengines.propdata.collection.entitymgr.RefreshProgressEntityMgr;
import com.latticeengines.propdata.core.source.DerivedSource;

@Component("refreshProgressEntityMgr")
public class RefreshProgressEntityMgrImpl
        extends AbstractProgressEntityMgr<RefreshProgress> implements RefreshProgressEntityMgr {

    private Log log = LogFactory.getLog(this.getClass());

    @Autowired
    RefreshProgressDao progressDao;

    @Override
    protected RefreshProgressDao getProgressDao() { return progressDao; }

    @Override
    protected Log getLog() { return log; }

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
