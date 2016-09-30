package com.latticeengines.propdata.engine.transformation.entitymgr;

import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.propdata.core.source.Source;

public interface TransformationProgressEntityMgr {

    TransformationProgress insertNewProgress(Source source, String version, String creator);

    TransformationProgress updateProgress(TransformationProgress progress);

    void deleteProgressByRootOperationUid(String rootOperationUid);

    TransformationProgress findProgressByRootOperationUid(String rootOperationUid);

    TransformationProgress findEarliestFailureUnderMaxRetry(Source source, String version);

    TransformationProgress findRunningProgress(Source source);

    TransformationProgress findRunningProgress(Source source, String version);

    boolean hasActiveForBaseSourceVersions(Source source, String baseSourceVersions);

    void deleteAllProgressesOfSource(Source source);

    TransformationProgress updateStatus(TransformationProgress progress, ProgressStatus status);

}
