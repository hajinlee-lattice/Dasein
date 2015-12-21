package com.latticeengines.propdata.collection.entitymanager.impl;

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgress;
import com.latticeengines.propdata.collection.dao.ArchiveProgressDao;
import com.latticeengines.propdata.collection.entitymanager.ArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.source.Source;

@Component("archiveProgressEntityMgr")
public class ArchiveProgressEntityMgrImpl
        extends AbstractProgressEntityMgr<ArchiveProgress> implements ArchiveProgressEntityMgr {

    private Log log = LogFactory.getLog(this.getClass());

    @Autowired
    ArchiveProgressDao progressDao;

    @Override
    protected ArchiveProgressDao getProgressDao() { return progressDao; }

    @Override
    protected Log getLog() { return log; }

    @Override
    @Transactional(value = "propDataCollectionProgress")
    public ArchiveProgress insertNewProgress(Source source, Date startDate, Date endDate, String creator) {
        try {
            ArchiveProgress newProgress = ArchiveProgress.constructByDates(source.getSourceName(), startDate, endDate);
            newProgress.setCreatedBy(creator);
            progressDao.create(newProgress);
            return newProgress;
        } catch (IllegalAccessException|InstantiationException e) {
            throw new RuntimeException(e);
        }
    }

}
