package com.latticeengines.datacloud.etl.transformation.entitymgr;

import java.util.List;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;

public interface TransformationProgressEntityMgr {

    TransformationProgress insertNewProgress(String pipelineName, Source source, String version, String creator);

    TransformationProgress updateProgress(TransformationProgress progress);

    void deleteProgressByRootOperationUid(String rootOperationUid);

    TransformationProgress findProgressByRootOperationUid(String rootOperationUid);

    TransformationProgress findEarliestFailureUnderMaxRetry(Source source, String version);

    TransformationProgress findRunningProgress(Source source);

    TransformationProgress findRunningProgress(Source source, String version);

    boolean hasActiveForBaseSourceVersions(Source source, String baseSourceVersions);

    void deleteAllProgressesOfSource(Source source);

    TransformationProgress updateStatus(TransformationProgress progress, ProgressStatus status);

    TransformationProgress findPipelineProgressAtVersion(String pipelineName, String version);

    void deleteProgress(TransformationProgress progress);

    List<TransformationProgress> findAllforPipeline(String pipelineName);

}
