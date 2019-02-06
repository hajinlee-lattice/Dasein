package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.listeners.SegmentExportListener;
import com.latticeengines.cdl.workflow.steps.SegmentExportInitStep;
import com.latticeengines.domain.exposed.serviceflows.cdl.SegmentExportWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.export.ExportData;
import com.latticeengines.serviceflows.workflow.export.ExportSegmentExportToS3;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("segmentExportWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SegmentExportWorkflow extends AbstractWorkflow<SegmentExportWorkflowConfiguration> {

    @Inject
    private SegmentExportInitStep segmentExportInitStep;

    @Inject
    private ExportData exportData;

    @Inject
    private ExportSegmentExportToS3 exportSegmentExportToS3;

    @Inject
    private SegmentExportListener segmentExportListener;

    @Override
    public Workflow defineWorkflow(SegmentExportWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config)//
                .next(segmentExportInitStep) //
                .next(exportData) //
                .next(exportSegmentExportToS3) //
                .listener(segmentExportListener) //
                .build();
    }
}
