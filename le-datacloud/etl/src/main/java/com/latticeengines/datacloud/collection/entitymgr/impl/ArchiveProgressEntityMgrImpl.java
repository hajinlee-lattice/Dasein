package com.latticeengines.datacloud.collection.entitymgr.impl;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private Logger log = LoggerFactory.getLogger(this.getClass());

    @Autowired
    ArchiveProgressDao progressDao;

    @Override
    protected ArchiveProgressDao getProgressDao() { return progressDao; }

    @Override
    protected Logger getLog() { return log; }

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
