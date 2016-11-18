package com.latticeengines.propdata.engine.transformation.service;

import java.util.List;

import com.latticeengines.datacloud.etl.transformation.entitymgr.TransformationProgressEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;

public interface TransformationExecutor {

    TransformationProgress kickOffNewProgress(TransformationProgressEntityMgr progressEntityMgr,
                                              List<String> baseVersions, String targetVersion);
    TransformationProgress kickOffNewPipelineProgress(TransformationProgressEntityMgr transformationProgressEntityMgr,
                                              PipelineTransformationRequest request);

    void purgeOldVersions();

}
