package com.latticeengines.proxy.exposed.dataflowapi;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.dataflow.DataFlowConfiguration;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component
public class DataFlowApiProxy extends MicroserviceRestApiProxy {
    public DataFlowApiProxy() {
        super("dataflowapi/dataflows/");
    }

    public AppSubmission submitDataFlowExecution(DataFlowConfiguration dataFlowConfig) {
        String url = constructUrl();
        return post("submitDataFlowExecution", url, dataFlowConfig, AppSubmission.class);
    }
}
