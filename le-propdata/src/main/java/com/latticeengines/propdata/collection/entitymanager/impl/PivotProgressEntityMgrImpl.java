package com.latticeengines.propdata.collection.entitymanager.impl;

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.propdata.collection.PivotProgress;
import com.latticeengines.propdata.collection.dao.PivotProgressDao;
import com.latticeengines.propdata.collection.entitymanager.PivotProgressEntityMgr;
import com.latticeengines.propdata.collection.source.PivotedSource;

@Component("pivotProgressEntityMgr")
public class PivotProgressEntityMgrImpl
        extends AbstractProgressEntityMgr<PivotProgress> implements PivotProgressEntityMgr {

    private Log log = LogFactory.getLog(this.getClass());

    @Autowired
    PivotProgressDao progressDao;

    @Override
    protected PivotProgressDao getProgressDao() { return progressDao; }

    @Override
    protected Log getLog() { return log; }

    @Override
    @Transactional(value = "propDataCollectionProgress")
    public PivotProgress insertNewProgress(PivotedSource source, Date pivotDate, String creator) {
        try {
            PivotProgress newProgress = PivotProgress.constructByDate(source.getSourceName(), pivotDate);
            newProgress.setCreatedBy(creator);
            progressDao.create(newProgress);
            return newProgress;
        } catch (IllegalAccessException|InstantiationException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @Transactional(value = "propDataCollectionProgress", readOnly = true)
    public PivotProgress findProgressByBaseVersion(PivotedSource source, String baseVersion) {
        return progressDao.findByBaseSourceVersion(source, baseVersion);
    }


}
