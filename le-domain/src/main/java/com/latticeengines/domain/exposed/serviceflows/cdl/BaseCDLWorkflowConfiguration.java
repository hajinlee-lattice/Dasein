package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Collection;
import java.util.Collections;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "name")
@JsonSubTypes({
        @Type(value = CDLDataFeedImportWorkflowConfiguration.class, name = "CDLDataFeedImportWorkflowConfiguration"),
        @Type(value = CDLImportWorkflowConfiguration.class, name = "CDLImportWorkflowConfiguration"),
        @Type(value = CdlMatchAndModelWorkflowConfiguration.class, name = "CdlMatchAndModelWorkflowConfiguration"),
        @Type(value = CDLOperationWorkflowConfiguration.class, name = "CDLOperationWorkflowConfiguration"),
        @Type(value = CustomEventMatchWorkflowConfiguration.class, name = "CustomEventMatchWorkflowConfiguration"),
        @Type(value = CustomEventModelingWorkflowConfiguration.class, name = "CustomEventModelingWorkflowConfiguration"),
        @Type(value = GenerateRatingWorkflowConfiguration.class, name = "GenerateRatingWorkflowConfiguration"),
        @Type(value = PlayLaunchWorkflowConfiguration.class, name = "PlayLaunchWorkflowConfiguration"),
        @Type(value = ProcessAnalyzeWorkflowConfiguration.class, name = "ProcessAnalyzeWorkflowConfiguration"),
        @Type(value = RatingEngineScoreWorkflowConfiguration.class, name = "RatingEngineScoreWorkflowConfiguration"),
        @Type(value = RedshiftPublishWorkflowConfiguration.class, name = "RedshiftPublishWorkflowConfiguration"),
        @Type(value = SegmentExportWorkflowConfiguration.class, name = "SegmentExportWorkflowConfiguration"), })
public class BaseCDLWorkflowConfiguration extends WorkflowConfiguration {

    @Override
    public Collection<String> getSwpkgNames() {
        return Collections.singleton(SoftwareLibrary.CDL.getName());
    }

}
