package com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.serviceflows.core.steps.AddStandardAttributesConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.DataFlowStepConfiguration;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "name")
@JsonSubTypes({ @Type(value = AddStandardAttributesConfiguration.class, name = "AddStandardAttributesConfiguration") })
public class BaseLPDataFlowStepConfiguration extends DataFlowStepConfiguration {

    @Override
    public String getSwlib() {
        return SoftwareLibrary.LeadPrioritization.getName();
    }

}
