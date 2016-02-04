package com.latticeengines.propdata.collection.entitymanager.impl;

import java.util.List;

import org.apache.commons.logging.Log;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.domain.exposed.propdata.manage.Progress;
import com.latticeengines.domain.exposed.propdata.manage.ProgressStatus;
import com.latticeengines.propdata.collection.dao.ProgressDao;
import com.latticeengines.propdata.collection.entitymanager.ProgressEntityMgr;
import com.latticeengines.propdata.core.source.Source;

public abstract class AbstractProgressEntityMgr<P extends Progress> implements ProgressEntityMgr<P> {

    protected abstract ProgressDao<P> getProgressDao();

    protected abstract Log getLog();

    private static final int MAX_RETRIES = 3;

    @Override
    @Transactional(value = "propDataManage")
    public void deleteProgressByRootOperationUid(String rootOperationUid) {
        P progress = getProgressDao().findByRootOperationUid(rootOperationUid);
        if (progress != null) {
            getProgressDao().delete(progress);
        }
    }

    @Override
    @Transactional(value = "propDataManage")
    public P updateStatus(P progress, ProgressStatus status) {
        progress.setStatus(status);
        return updateProgress(progress);
    }

    @Override
    @Transactional(value = "propDataManage")
    public P updateProgress(P progress) {
        getProgressDao().update(progress);
        return progress;
    }

    @Override
    @Transactional(value = "propDataManage", readOnly = true)
    public P findProgressByRootOperationUid(String rootOperationUid) {
        return getProgressDao().findByRootOperationUid(rootOperationUid);
    }

    @Override
    @Transactional(value = "propDataManage", readOnly = true)
    public P findEarliestFailureUnderMaxRetry(Source source) {
        List<P> progresses = getProgressDao().findFailedProgresses(source);
        for (P progress : progresses) {
            if (progress.getNumRetries() < MAX_RETRIES) {
                return progress;
            }
        }
        return null;
    }

    @Override
    @VisibleForTesting
    @Transactional(value = "propDataManage")
    public void deleteAllProgressesOfSource(Source source) {
        List<P> progresses = getProgressDao().findAllOfSource(source);
        for (P progress : progresses) {
            getProgressDao().delete(progress);
        }
    }

    @Override
    @Transactional(value = "propDataManage", readOnly = true)
    public P findRunningProgress(Source source) {
        List<P> progresses = getProgressDao().findUnfinishedProgresses(source);
        if (!progresses.isEmpty()) {
            return progresses.get(0);
        } else {
            return null;
        }
    }

}
