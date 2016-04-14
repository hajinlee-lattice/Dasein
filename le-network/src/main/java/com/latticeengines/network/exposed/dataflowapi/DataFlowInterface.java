package com.latticeengines.network.exposed.dataflowapi;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.dataflow.DataFlowConfiguration;

public interface DataFlowInterface {
    AppSubmission submitDataFlowExecution(DataFlowConfiguration dataFlowConfig);
}
