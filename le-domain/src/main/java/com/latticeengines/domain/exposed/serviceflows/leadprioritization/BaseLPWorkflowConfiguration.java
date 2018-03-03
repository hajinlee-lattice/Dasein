package com.latticeengines.domain.exposed.serviceflows.leadprioritization;

import java.util.Collection;
import java.util.Collections;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "name")
@JsonSubTypes({
        @Type(value = ImportAndRTSBulkScoreWorkflowConfiguration.class, name = "ImportAndRTSBulkScoreWorkflowConfiguration"),
        @Type(value = ImportMatchAndModelWorkflowConfiguration.class, name = "ImportMatchAndModelWorkflowConfiguration"),
        @Type(value = ImportMatchAndScoreWorkflowConfiguration.class, name = "ImportMatchAndScoreWorkflowConfiguration"),
        @Type(value = ImportVdbTableAndPublishWorkflowConfiguration.class, name = "ImportVdbTableAndPublishWorkflowConfiguration"),
        @Type(value = MatchAndModelWorkflowConfiguration.class, name = "MatchAndModelWorkflowConfiguration"), })
public class BaseLPWorkflowConfiguration extends WorkflowConfiguration {

    @Override
    public Collection<String> getSwpkgNames() {
        return Collections.singleton(SoftwareLibrary.LeadPrioritization.getName());
    }

}
