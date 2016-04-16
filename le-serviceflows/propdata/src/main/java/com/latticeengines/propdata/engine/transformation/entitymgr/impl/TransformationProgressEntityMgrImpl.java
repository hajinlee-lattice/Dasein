package com.latticeengines.propdata.engine.transformation.entitymgr.impl;

import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.manage.TransformationProgress;
import com.latticeengines.domain.exposed.propdata.manage.TransformationProgressStatus;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.engine.transformation.dao.TransformationProgressDao;
import com.latticeengines.propdata.engine.transformation.entitymgr.TransformationProgressEntityMgr;

@Component("transformationProgressEntityMgr")
public class TransformationProgressEntityMgrImpl implements TransformationProgressEntityMgr {

    private static final long TIME_24_HOUR_IN_MILLISECONDS = 24 * 60 * 60 * 1000L;
    private static final int MAX_RETRIES = 3;

    private static final Log log = LogFactory.getLog(TransformationProgressEntityMgrImpl.class);

    @Autowired
    private TransformationProgressDao progressDao;

    @Override
    @Transactional(value = "propDataManage")
    public TransformationProgress insertNewProgress(Source source, String version, String creator) {
        try {
            Date currentDate = new Date();
            Date endDate = new Date(currentDate.getTime() + TIME_24_HOUR_IN_MILLISECONDS);
            TransformationProgress newProgress = TransformationProgress.constructByDates(source.getSourceName(),
                    currentDate, endDate);
            newProgress.setCreatedBy(creator);
            newProgress.setVersion(version);
            progressDao.create(newProgress);
            return newProgress;
        } catch (IllegalAccessException | InstantiationException e) {
            throw new LedpException(LedpCode.LEDP_25014, e);
        }
    }

    @Override
    @Transactional(value = "propDataManage")
    public void deleteProgressByRootOperationUid(String rootOperationUid) {
        TransformationProgress progress = progressDao.findByRootOperationUid(rootOperationUid);
        if (progress != null) {
            progressDao.delete(progress);
        }
    }

    @Override
    @Transactional(value = "propDataManage")
    public TransformationProgress updateStatus(TransformationProgress progress, TransformationProgressStatus status) {
        progress.setStatus(status);
        return updateProgress(progress);
    }

    @Override
    @Transactional(value = "propDataManage")
    public TransformationProgress updateProgress(TransformationProgress progress) {
        progressDao.update(progress);
        return progress;
    }

    @Override
    @Transactional(value = "propDataManage", readOnly = true)
    public TransformationProgress findProgressByRootOperationUid(String rootOperationUid) {
        return progressDao.findByRootOperationUid(rootOperationUid);
    }

    @Override
    @Transactional(value = "propDataManage", readOnly = true)
    public TransformationProgress findEarliestFailureUnderMaxRetry(Source source) {
        List<TransformationProgress> progresses = progressDao.findFailedProgresses(source);
        for (TransformationProgress progress : progresses) {
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
        List<TransformationProgress> progresses = progressDao.findAllOfSource(source);
        for (TransformationProgress progress : progresses) {
            progressDao.delete(progress);
        }
    }

    @Override
    @Transactional(value = "propDataManage", readOnly = true)
    public TransformationProgress findRunningProgress(Source source) {
        List<TransformationProgress> progresses = progressDao.findUnfinishedProgresses(source);
        if (!progresses.isEmpty()) {
            return progresses.get(0);
        } else {
            return null;
        }
    }
}
