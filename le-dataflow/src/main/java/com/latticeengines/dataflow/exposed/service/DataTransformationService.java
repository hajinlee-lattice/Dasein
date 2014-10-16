package com.latticeengines.dataflow.exposed.service;

import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

public interface DataTransformationService {

    void executeNamedTransformation(DataFlowContext dataFlowCtx, String dataFlowBldrBeanName);
}
