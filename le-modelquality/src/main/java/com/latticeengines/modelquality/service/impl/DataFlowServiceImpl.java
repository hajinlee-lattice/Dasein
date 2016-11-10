package com.latticeengines.modelquality.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dataflow.flows.leadprioritization.DedupType;
import com.latticeengines.domain.exposed.modelquality.DataFlow;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.modelquality.entitymgr.DataFlowEntityMgr;
import com.latticeengines.modelquality.service.DataFlowService;

@Component("modelQualityDataFlowService")
public class DataFlowServiceImpl extends BaseServiceImpl implements DataFlowService {

    @Autowired
    private DataFlowEntityMgr dataFlowEntityMgr;

    @Override
    public DataFlow createLatestProductionDataFlow() {
        String version = getVersion();
        String dataFlowName = "PRODUCTION-" + version;
        DataFlow dataFlow = dataFlowEntityMgr.findByName(dataFlowName);

        if (dataFlow != null) {
            return dataFlow;
        }

        dataFlow = new DataFlow();
        dataFlow.setName(dataFlowName);
        dataFlow.setMatch(true);
        dataFlow.setTransformationGroup(TransformationGroup.STANDARD);
        dataFlow.setDedupType(DedupType.MULTIPLELEADSPERDOMAIN);
        dataFlowEntityMgr.create(dataFlow);
        return dataFlow;
    }

}
