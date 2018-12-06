package com.latticeengines.domain.exposed.serviceflows.datacloud;

import java.util.Collection;
import java.util.Collections;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.IngestionWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.PublishWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.BulkMatchWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.CascadingBulkMatchWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.CommitEntityMatchWorkflowConfiguration;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "name")
@JsonSubTypes({
        @Type(value = BulkMatchWorkflowConfiguration.class, name = "BulkMatchWorkflowConfiguration"),
        @Type(value = CascadingBulkMatchWorkflowConfiguration.class, name = "CascadingBulkMatchWorkflowConfiguration"),
        @Type(value = IngestionWorkflowConfiguration.class, name = "IngestionWorkflowConfiguration"),
        @Type(value = PublishWorkflowConfiguration.class, name = "PublishWorkflowConfiguration"),
        @Type(value = TransformationWorkflowConfiguration.class, name = "TransformationWorkflowConfiguration"),
        @Type(value = CommitEntityMatchWorkflowConfiguration.class, name = "CommitEntityMatchWorkflowConfiguration"),
        @Type(value = MatchDataCloudWorkflowConfiguration.class, name = "MatchDataCloudWorkflowConfiguration"), })
public class BaseDataCloudWorkflowConfiguration extends WorkflowConfiguration {

    @Override
    public Collection<String> getSwpkgNames() {
        return Collections.singleton(SoftwareLibrary.DataCloud.getName());
    }

}
