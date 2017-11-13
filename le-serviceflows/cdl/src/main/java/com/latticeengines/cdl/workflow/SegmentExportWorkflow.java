package com.latticeengines.cdl.workflow;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.listeners.SegmentExportListener;
import com.latticeengines.cdl.workflow.steps.SegmentExportInitStep;
import com.latticeengines.domain.exposed.serviceflows.cdl.SegmentExportWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.export.ExportData;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("segmentExportWorkflow")
public class SegmentExportWorkflow extends AbstractWorkflow<SegmentExportWorkflowConfiguration> {

    @Autowired
    private SegmentExportInitStep segmentExportInitStep;

    @Autowired
    private ExportData exportData;

    @Autowired
    private SegmentExportListener segmentExportListener;

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder().next(segmentExportInitStep) //
                .next(exportData) //
                .listener(segmentExportListener) //
                .build();
    }
}
