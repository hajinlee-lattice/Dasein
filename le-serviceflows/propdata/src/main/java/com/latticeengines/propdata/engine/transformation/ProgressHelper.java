package com.latticeengines.propdata.engine.transformation;

import org.apache.commons.logging.Log;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.manage.TransformationProgress;
import com.latticeengines.domain.exposed.propdata.manage.TransformationProgressStatus;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.core.util.LoggingUtils;
import com.latticeengines.propdata.engine.transformation.entitymgr.TransformationProgressEntityMgr;

@Component
public class ProgressHelper {

    public TransformationProgress findRunningJob(TransformationProgressEntityMgr progressEntityMgr, Source source) {
        return progressEntityMgr.findRunningProgress(source);
    }

    public TransformationProgress findJobToRetry(TransformationProgressEntityMgr progressEntityManager, Source source, String version) {
        return progressEntityManager.findEarliestFailureUnderMaxRetry(source, version);
    }

    public boolean checkProgressStatus(TransformationProgress progress, Log logger) {
        if (progress != null && (TransformationProgressStatus.NEW.equals(progress.getStatus())
                || TransformationProgressStatus.FAILED.equals(progress.getStatus()))) {
            return true;
        }

        return false;
    }

    public void logIfRetrying(TransformationProgress progress, Log logger) {
        if (progress.getStatus().equals(TransformationProgressStatus.FAILED)) {
            int numRetries = progress.getNumRetries() + 1;
            progress.setNumRetries(numRetries);
            LoggingUtils.logInfo(logger, progress, String.format("Retry [%d] from [%s].", progress.getNumRetries(),
                    TransformationProgressStatus.FAILED));
        }
    }

    public void updateStatusToFailed(TransformationProgressEntityMgr progressEntityManager,
            TransformationProgress progress, String errorMsg, Exception e, Log logger) {
        LoggingUtils.logError(logger, progress, errorMsg, e);
        progress.setErrorMessage(errorMsg);
        progressEntityManager.updateStatus(progress, TransformationProgressStatus.FAILED);
    }

    public TransformationProgress finishProgress(TransformationProgressEntityMgr progressEntityManager,
            TransformationProgress progress, Log logger) {
        progress.setNumRetries(0);
        LoggingUtils.logInfo(logger, progress, "Transformed.");
        return progressEntityManager.updateStatus(progress, TransformationProgressStatus.FINISHED);
    }

}
