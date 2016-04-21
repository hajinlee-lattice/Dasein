package com.latticeengines.propdata.engine.transformation.service;

import java.util.List;

import com.latticeengines.domain.exposed.propdata.manage.TransformationProgress;
import com.latticeengines.domain.exposed.propdata.transformation.TransformationRequest;

public interface SourceTransformationService {

    List<TransformationProgress> scan(String hdfsPod);

    TransformationProgress transform(TransformationRequest request, String hdfsPod);

}
