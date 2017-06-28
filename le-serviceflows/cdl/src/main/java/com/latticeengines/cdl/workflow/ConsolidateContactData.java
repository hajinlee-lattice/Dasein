package com.latticeengines.cdl.workflow;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.ConsolidateDataConfiguration;

@Component("consolidateContactData")
public class ConsolidateContactData extends ConsolidateDataBase<ConsolidateDataConfiguration> {

    @Override
    public PipelineTransformationRequest getConsolidateRequest() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public BusinessEntity getBusinessEntity() {
        return BusinessEntity.Contact;
    }

    @Override
    public boolean isBucketing() {
        // TODO Auto-generated method stub
        return false;
    }

}
