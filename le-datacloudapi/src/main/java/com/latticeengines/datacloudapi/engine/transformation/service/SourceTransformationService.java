package com.latticeengines.datacloudapi.engine.transformation.service;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.TransformationRequest;

public interface SourceTransformationService {

    List<TransformationProgress> scan(String hdfsPod);

    TransformationProgress transform(TransformationRequest request, String hdfsPod, boolean fromScan);

    TransformationProgress pipelineTransform(PipelineTransformationRequest request, String hdfsPod);

    TransformationProgress getProgress(String rootOperationUid); 
}
