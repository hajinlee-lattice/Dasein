package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.ExportTimelineStep;
import com.latticeengines.cdl.workflow.steps.GenerateTimelineExportUniverse;
import com.latticeengines.domain.exposed.serviceflows.cdl.TimelineExportWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.export.ExportTimelineToS3;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("timelineExportWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class TimelineExportWorkflow extends AbstractWorkflow<TimelineExportWorkflowConfiguration> {

    @Inject
    private GenerateTimelineExportUniverse generateTimelineExportUniverse;
    @Inject
    private ExportTimelineStep exportTimelineStep;
    @Inject
    private ExportTimelineToS3 exportTimelineToS3;

    @Override
    public Workflow defineWorkflow(TimelineExportWorkflowConfiguration workflowConfig) {
        return new WorkflowBuilder(name(), workflowConfig)
                .next(generateTimelineExportUniverse)
                .next(exportTimelineStep)
                .next(exportTimelineToS3)
                .build();
    }
}
