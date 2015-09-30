package com.latticeengines.dataflowapi.service;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.dataflow.DataFlowConfiguration;

public interface DataFlowService {

    ApplicationId submitDataFlow(DataFlowConfiguration dataFlowConfig);
}
