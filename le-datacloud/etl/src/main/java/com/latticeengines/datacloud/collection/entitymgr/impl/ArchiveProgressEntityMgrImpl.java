package com.latticeengines.datacloud.collection.entitymgr.impl;

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.datacloud.collection.dao.ArchiveProgressDao;
import com.latticeengines.datacloud.collection.entitymgr.ArchiveProgressEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.domain.exposed.datacloud.manage.ArchiveProgress;

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
    @Transactional(value = "propDataManage")
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
