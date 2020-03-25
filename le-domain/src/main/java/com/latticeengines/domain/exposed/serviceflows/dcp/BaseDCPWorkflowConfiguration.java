package com.latticeengines.domain.exposed.serviceflows.dcp;

import java.util.Collection;
import java.util.Collections;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "name")
@JsonSubTypes({
        @JsonSubTypes.Type(value = DCPSourceImportWorkflowConfiguration.class, name = "DCPSourceImportWorkflowConfiguration")})
public class BaseDCPWorkflowConfiguration extends WorkflowConfiguration {

    @Override
    public Collection<String> getSwpkgNames() {
        return Collections.singleton(SoftwareLibrary.DCP.getName());
    }

}
