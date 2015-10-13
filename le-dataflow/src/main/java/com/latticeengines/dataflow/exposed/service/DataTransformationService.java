package com.latticeengines.dataflow.exposed.service;

import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.metadata.Table;

public interface DataTransformationService {

    Table executeNamedTransformation(DataFlowContext dataFlowCtx, String dataFlowBldrBeanName);
}
