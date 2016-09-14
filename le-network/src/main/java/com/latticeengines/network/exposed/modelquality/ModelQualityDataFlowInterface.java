package com.latticeengines.network.exposed.modelquality;

import java.util.List;

import com.latticeengines.domain.exposed.modelquality.DataFlow;

public interface ModelQualityDataFlowInterface {

    List<DataFlow> getDataFlows();

    String createDataFlow(DataFlow dataflow);

    DataFlow getDataFlowByName(String dataFlowName);

    DataFlow createDataFlowFromProduction();

}
