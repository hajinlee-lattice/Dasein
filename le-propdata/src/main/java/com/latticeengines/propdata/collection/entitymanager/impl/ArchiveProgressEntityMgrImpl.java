package com.latticeengines.propdata.collection.entitymanager.impl;

import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgress;
import com.latticeengines.domain.exposed.propdata.collection.ProgressStatus;
import com.latticeengines.propdata.collection.dao.ArchiveProgressDao;
import com.latticeengines.propdata.collection.entitymanager.ArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.source.Source;

@Component("archiveProgressEntityMgr")
public class ArchiveProgressEntityMgrImpl
        extends AbstractProgressEntityMgr<ArchiveProgress> implements ArchiveProgressEntityMgr {

    @Autowired
    ArchiveProgressDao progressDao;

    private static final int MAX_RETRIES = 3;

    @Override
    protected ArchiveProgressDao getProgressDao() { return progressDao; }

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

    @Override
    @Transactional(value = "propDataCollectionProgress", readOnly = true)
    public ArchiveProgress findEarliestFailureUnderMaxRetry(Source source) {
        List<ArchiveProgress> progresses = progressDao.findFailedProgresses(source);
        for (ArchiveProgress progress: progresses) {
            if (progress.getNumRetries() < MAX_RETRIES) {
                return progress;
            }
        }
        return null;
    }

    @Override
    @Transactional(value = "propDataCollectionProgress", readOnly = true)
    public ArchiveProgress findProgressNotInFinalState(Source source) {
        Set<ProgressStatus> finalStatus =
                new HashSet<>(Arrays.asList(ProgressStatus.UPLOADED, ProgressStatus.FAILED));
        List<ArchiveProgress> progresses = progressDao.findAllOfSource(source);
        for (ArchiveProgress progress: progresses) {
            if (!finalStatus.contains(progress.getStatus())) {
                return progress;
            }
        }
        return null;
    }

}
