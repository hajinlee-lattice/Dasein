package com.latticeengines.network.exposed.propdata;

import java.util.List;

import com.latticeengines.domain.exposed.propdata.manage.TransformationProgress;
import com.latticeengines.domain.exposed.propdata.transformation.TransformationRequest;

public interface TransformationInterface {

	List<TransformationProgress> scan(String hdfsPod);

	TransformationProgress transform(TransformationRequest transformationRequest, String hdfsPod);

}
