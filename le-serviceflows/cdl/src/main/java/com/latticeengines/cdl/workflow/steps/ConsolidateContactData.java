package com.latticeengines.cdl.workflow.steps;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.ConsolidateContactDataStepConfiguration;

@Component("consolidateContactData")
public class ConsolidateContactData extends ConsolidateDataBase<ConsolidateContactDataStepConfiguration> {

    @Override
    public PipelineTransformationRequest getConsolidateRequest() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isBucketing() {
        // TODO Auto-generated method stub
        return false;
    }

}
