package com.latticeengines.propdata.collection.entitymanager.impl;

import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.propdata.collection.Progress;
import com.latticeengines.domain.exposed.propdata.collection.ProgressStatus;
import com.latticeengines.propdata.collection.dao.ProgressDao;
import com.latticeengines.propdata.collection.entitymanager.ProgressEntityMgr;

public abstract class AbstractProgressEntityMgr<P extends Progress> implements ProgressEntityMgr<P> {

    protected abstract ProgressDao<P> getProgressDao();

    @Override
    @Transactional(value = "propDataCollectionProgress")
    public void deleteProgressByRootOperationUid(String rootOperationUid) {
        P progress = getProgressDao().findByRootOperationUid(rootOperationUid);
        if (progress != null) {
            getProgressDao().delete(progress);
        }
    }

    @Override
    @Transactional(value = "propDataCollectionProgress")
    public P updateStatus(P progress, ProgressStatus status) {
        progress.setStatus(status);
        return updateProgress(progress);
    }

    @Override
    @Transactional(value = "propDataCollectionProgress")
    public P updateProgress(P progress) {
        getProgressDao().update(progress);
        return progress;
    }

    @Override
    @Transactional(value = "propDataCollectionProgress", readOnly = true)
    public P findProgressByRootOperationUid(String rootOperationUid) {
        return getProgressDao().findByRootOperationUid(rootOperationUid);
    }

}
