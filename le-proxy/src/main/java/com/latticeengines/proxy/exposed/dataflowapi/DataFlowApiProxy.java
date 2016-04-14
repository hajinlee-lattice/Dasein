package com.latticeengines.proxy.exposed.dataflowapi;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.dataflow.DataFlowConfiguration;
import com.latticeengines.network.exposed.dataflowapi.DataFlowInterface;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component
public class DataFlowApiProxy extends BaseRestApiProxy implements DataFlowInterface {
    public DataFlowApiProxy() {
        super("dataflowapi/dataflows/");
    }

    @Override
    public AppSubmission submitDataFlowExecution(DataFlowConfiguration dataFlowConfig) {
        String url = constructUrl();
        return post("submitDataFlowExecution", url, dataFlowConfig, AppSubmission.class);
    }
}
