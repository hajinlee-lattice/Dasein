package com.latticeengines.domain.exposed.serviceflows.modeling;

import java.util.Collection;
import java.util.Collections;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "name")
@JsonSubTypes({
        @Type(value = ModelWorkflowConfiguration.class, name = "ModelWorkflowConfiguration"),
        @Type(value = PMMLModelWorkflowConfiguration.class, name = "PMMLModelWorkflowConfiguration"),
        @Type(value = ModelDataValidationWorkflowConfiguration.class, name = "ModelDataValidationWorkflowConfiguration"), })
public class BaseModelingWorkflowConfiguration extends WorkflowConfiguration {

    @Override
    public Collection<String> getSwpkgNames() {
        return Collections.singleton(SoftwareLibrary.Modeling.getName());
    }

}
