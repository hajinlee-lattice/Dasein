package com.latticeengines.datacloud.etl.transformation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.util.LoggingUtils;
import com.latticeengines.datacloud.etl.transformation.entitymgr.TransformationProgressEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;

@Component
public class ProgressHelper {

    private static final Logger log = LoggerFactory.getLogger(ProgressHelper.class);

    public TransformationProgress findRunningJob(TransformationProgressEntityMgr progressEntityMgr, Source source) {
        return progressEntityMgr.findRunningProgress(source);
    }

    public TransformationProgress findJobToRetry(TransformationProgressEntityMgr progressEntityManager, Source source, String version) {
        return progressEntityManager.findEarliestFailureUnderMaxRetry(source, version);
    }

    public boolean checkProgressStatus(TransformationProgress progress) {
        return progress != null && (ProgressStatus.NEW.equals(progress.getStatus())
                || ProgressStatus.FAILED.equals(progress.getStatus()));
    }

    public void logIfRetrying(TransformationProgress progress, String clzName) {
        if (progress.getStatus().equals(ProgressStatus.FAILED)) {
            int numRetries = progress.getNumRetries() + 1;
            progress.setNumRetries(numRetries);
            log.info(LoggingUtils.log(clzName, progress, String.format("Retry [%d] from [%s].", progress.getNumRetries(),
                    ProgressStatus.FAILED)));
        }
    }

    public void updateStatusToFailed(TransformationProgressEntityMgr progressEntityManager,
            TransformationProgress progress, String errorMsg, Exception e, String clzName) {
        if (e == null) {
            log.error(LoggingUtils.log(clzName, progress, errorMsg));
        } else {
            log.error(LoggingUtils.log(clzName, progress, errorMsg), e);
        }
        progress.setErrorMessage(errorMsg);
        progressEntityManager.updateStatus(progress, ProgressStatus.FAILED);
    }

    public TransformationProgress finishProgress(TransformationProgressEntityMgr progressEntityManager,
            TransformationProgress progress, String clzName) {
        progress.setNumRetries(0);
        log.info(LoggingUtils.log(clzName, progress, "Transformed."));
        return progressEntityManager.updateStatus(progress, ProgressStatus.FINISHED);
    }

}
