package com.latticeengines.domain.exposed.serviceflows.prospectdiscovery;

import java.util.Collection;
import java.util.Collections;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "name")
@JsonSubTypes({
        @Type(value = CreateAttributeLevelSummaryWorkflowConfiguration.class, name = "CreateAttributeLevelSummaryWorkflowConfiguration"),
        @Type(value = FitModelWorkflowConfiguration.class, name = "FitModelWorkflowConfiguration"), })
public class BasePDWorkflowConfiguration extends WorkflowConfiguration {

    @Override
    public Collection<String> getSwpkgNames() {
        return Collections.singleton(SoftwareLibrary.ProspectDiscovery.getName());
    }

}
