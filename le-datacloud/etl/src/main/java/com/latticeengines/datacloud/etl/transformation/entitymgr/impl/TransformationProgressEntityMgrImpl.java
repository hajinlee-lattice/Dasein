package com.latticeengines.datacloud.etl.transformation.entitymgr.impl;

import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.etl.transformation.dao.TransformationProgressDao;
import com.latticeengines.datacloud.etl.transformation.entitymgr.TransformationProgressEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component("transformationProgressEntityMgr")
public class TransformationProgressEntityMgrImpl implements TransformationProgressEntityMgr {

    private static final long TIME_24_HOUR_IN_MILLISECONDS = 24 * 60 * 60 * 1000L;
    private static final int MAX_RETRIES = 3;

    @Autowired
    private TransformationProgressDao progressDao;

    @Override
    @Transactional(value = "propDataManage")
    public TransformationProgress insertNewProgress(String pipelineName, Source source, String version,
            String creator) {
        try {
            Date currentDate = new Date();
            Date endDate = new Date(currentDate.getTime() + TIME_24_HOUR_IN_MILLISECONDS);
            TransformationProgress newProgress = TransformationProgress.constructByDates(source.getSourceName(),
                    currentDate, endDate);
            newProgress.setCreatedBy(creator);
            newProgress.setVersion(version);
            newProgress.setHdfsPod(HdfsPodContext.getHdfsPodId());
            newProgress.setPipelineName(pipelineName);
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
    public TransformationProgress updateStatus(TransformationProgress progress, ProgressStatus status) {
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
    public TransformationProgress findEarliestFailureUnderMaxRetry(Source source, String version) {
        List<TransformationProgress> progresses = progressDao.findFailedProgresses(source);
        for (TransformationProgress progress : progresses) {
            if (progress.getNumRetries() < MAX_RETRIES) {
                if (version != null && !version.equals(progress.getVersion())) {
                    continue;
                }
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

    @Override
    @Transactional(value = "propDataManage", readOnly = true)
    public TransformationProgress findRunningProgress(Source source, String version) {
        List<TransformationProgress> progresses = progressDao.findUnfinishedProgresses(source);
        if (!progresses.isEmpty()) {
            for (TransformationProgress progress : progresses) {
                if (source.getSourceName().equals(progress.getSourceName())) {
                    return progress;
                }
            }
            return null;
        } else {
            return null;
        }
    }

    @Override
    @Transactional(value = "propDataManage", readOnly = true)
    public boolean hasActiveForBaseSourceVersions(Source source, String baseSourceVersions) {
        List<TransformationProgress> progresses = progressDao.findAllForBaseSourceVersions(source.getSourceName(),
                baseSourceVersions);

        if (progresses == null || progresses.isEmpty()) {
            return false;
        }

        // active means either finished, running or failed but under max retries
        for (TransformationProgress progress: progresses) {
            if (ProgressStatus.FINISHED.equals(progress.getStatus()) || ProgressStatus.PROCESSING.equals(progress.getStatus())) {
                return true;
            }
            if (ProgressStatus.FAILED.equals(progress.getStatus()) && progress.getNumRetries() <= MAX_RETRIES) {
                return true;
            }
        }

        return false;
    }

    @Override
    @Transactional(value = "propDataManage", readOnly = true)
    public TransformationProgress findPipelineProgressAtVersion(String pipelineName, String version) {
        return progressDao.findPipelineAtVersion(pipelineName, version);
    }

    @Override
    @Transactional(value = "propDataManage")
    public void deleteProgress(TransformationProgress progress) {
        progressDao.delete(progress);
    }

    @Override
    @Transactional(value = "propDataManage")
    public List<TransformationProgress> findAllforPipeline(String pipelineName) {
        return progressDao.findAllforPipeline(pipelineName);
    }
}
