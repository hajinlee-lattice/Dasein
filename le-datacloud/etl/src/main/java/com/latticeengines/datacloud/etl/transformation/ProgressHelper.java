package com.latticeengines.datacloud.etl.transformation;

import org.slf4j.Logger;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.util.LoggingUtils;
import com.latticeengines.datacloud.etl.transformation.entitymgr.TransformationProgressEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;

@Component
public class ProgressHelper {

    public TransformationProgress findRunningJob(TransformationProgressEntityMgr progressEntityMgr, Source source) {
        return progressEntityMgr.findRunningProgress(source);
    }

    public TransformationProgress findJobToRetry(TransformationProgressEntityMgr progressEntityManager, Source source, String version) {
        return progressEntityManager.findEarliestFailureUnderMaxRetry(source, version);
    }

    public boolean checkProgressStatus(TransformationProgress progress, Logger logger) {
        if (progress != null && (ProgressStatus.NEW.equals(progress.getStatus())
                || ProgressStatus.FAILED.equals(progress.getStatus()))) {
            return true;
        }

        return false;
    }

    public void logIfRetrying(TransformationProgress progress, Logger logger) {
        if (progress.getStatus().equals(ProgressStatus.FAILED)) {
            int numRetries = progress.getNumRetries() + 1;
            progress.setNumRetries(numRetries);
            LoggingUtils.logInfo(logger, progress, String.format("Retry [%d] from [%s].", progress.getNumRetries(),
                    ProgressStatus.FAILED));
        }
    }

    public void updateStatusToFailed(TransformationProgressEntityMgr progressEntityManager,
            TransformationProgress progress, String errorMsg, Exception e, Logger logger) {
        LoggingUtils.logError(logger, progress, errorMsg, e);
        progress.setErrorMessage(errorMsg);
        progressEntityManager.updateStatus(progress, ProgressStatus.FAILED);
    }

    public TransformationProgress finishProgress(TransformationProgressEntityMgr progressEntityManager,
            TransformationProgress progress, Logger logger) {
        progress.setNumRetries(0);
        LoggingUtils.logInfo(logger, progress, "Transformed.");
        return progressEntityManager.updateStatus(progress, ProgressStatus.FINISHED);
    }

}
