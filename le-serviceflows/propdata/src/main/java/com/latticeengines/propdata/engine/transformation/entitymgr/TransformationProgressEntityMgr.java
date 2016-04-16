package com.latticeengines.propdata.engine.transformation.entitymgr;

import com.latticeengines.domain.exposed.propdata.manage.TransformationProgress;
import com.latticeengines.domain.exposed.propdata.manage.TransformationProgressStatus;
import com.latticeengines.propdata.core.source.Source;

public interface TransformationProgressEntityMgr {

    TransformationProgress insertNewProgress(Source source, String version, String creator);

    TransformationProgress updateProgress(TransformationProgress progress);

    void deleteProgressByRootOperationUid(String rootOperationUid);

    TransformationProgress findProgressByRootOperationUid(String rootOperationUid);

    TransformationProgress findEarliestFailureUnderMaxRetry(Source source);

    TransformationProgress findRunningProgress(Source source);

    void deleteAllProgressesOfSource(Source source);

    TransformationProgress updateStatus(TransformationProgress progress, TransformationProgressStatus status);

}
