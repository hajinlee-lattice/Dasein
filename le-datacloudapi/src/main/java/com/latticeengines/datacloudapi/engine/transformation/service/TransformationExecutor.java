package com.latticeengines.datacloudapi.engine.transformation.service;

import java.util.List;

import com.latticeengines.datacloud.etl.transformation.entitymgr.TransformationProgressEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;

public interface TransformationExecutor {

    TransformationProgress kickOffNewProgress(TransformationProgressEntityMgr progressEntityMgr,
                                              List<String> baseVersions, String targetVersion);
    TransformationProgress kickOffNewPipelineProgress(TransformationProgressEntityMgr transformationProgressEntityMgr,
                                                      PipelineTransformationRequest request);

    TransformationWorkflowConfiguration generateNewPipelineWorkflowConf(PipelineTransformationRequest request);

    void purgeOldVersions();

}
