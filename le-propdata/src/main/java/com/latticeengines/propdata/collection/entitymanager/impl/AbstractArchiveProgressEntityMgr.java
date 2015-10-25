package com.latticeengines.propdata.collection.entitymanager.impl;

import java.util.Date;
import java.util.List;

import javax.annotation.PostConstruct;

import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgressBase;
import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgressStatus;
import com.latticeengines.propdata.collection.dao.ArchiveProgressDao;
import com.latticeengines.propdata.collection.entitymanager.ArchiveProgressEntityMgr;

public abstract class AbstractArchiveProgressEntityMgr<T extends ArchiveProgressBase> implements ArchiveProgressEntityMgr<T> {

    ArchiveProgressDao<T> progressDao;

    private static final int MAX_RETRIES = 5;

    abstract ArchiveProgressDao<T> getProgressDao();

    @PostConstruct
    void setProgressDao() {
        progressDao = getProgressDao();
    }

    @Override
    @Transactional(value = "propDataCollectionDest")
    public T insertNewProgress(Date startDate, Date endDate, String creator)
            throws InstantiationException, IllegalAccessException {
        T newProgress = ArchiveProgressBase.constructByDates(startDate, endDate, getProgressClass());
        newProgress.setCreatedBy(creator);
        progressDao.create(newProgress);
        return newProgress;
    }

    @Override
    @Transactional(value = "propDataCollectionDest")
    public void deleteProgressByRootOperationUid(String rootOperationUid) {
        T progress = progressDao.findByRootOperationUid(rootOperationUid);
        if (progress != null) {
            progressDao.delete(progress);
        }
    }

    @Override
    @Transactional(value = "propDataCollectionDest")
    public T updateStatus(T progress, ArchiveProgressStatus status) {
        progress.setStatus(status);
        return updateProgress(progress);
    }

    @Override
    @Transactional(value = "propDataCollectionDest")
    public T updateProgress(T progress) {
        progressDao.update(progress);
        return progress;
    }

    @Override
    @Transactional(value = "propDataCollectionDest", readOnly = true)
    public T findProgressByRootOperationUid(String rootOperationUid) {
        return progressDao.findByRootOperationUid(rootOperationUid);
    }

    @Override
    @Transactional(value = "propDataCollectionDest", readOnly = true)
    public T findEarliestFailureUnderMaxRetry() {
        List<T> progresses = progressDao.findFailedProgresses();
        for (T progress: progresses) {
            if (progress.getNumRetries() < MAX_RETRIES) {
                return progress;
            }
        }
        return null;
    }

}
