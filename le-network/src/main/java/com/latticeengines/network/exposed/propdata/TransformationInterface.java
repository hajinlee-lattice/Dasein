package com.latticeengines.network.exposed.propdata;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.TransformationRequest;

public interface TransformationInterface {

	List<TransformationProgress> scan(String hdfsPod);

	TransformationProgress transform(TransformationRequest transformationRequest, String hdfsPod);

    TransformationProgress transform(PipelineTransformationRequest transformationRequest, String hdfsPod);

    TransformationProgress getProgress(String rootOperationUid);

}
