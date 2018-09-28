package com.latticeengines.cdl.workflow;

import com.latticeengines.cdl.workflow.listeners.SegmentExportListener;
import com.latticeengines.cdl.workflow.steps.OrphanRecordExportStep;
import com.latticeengines.domain.exposed.serviceflows.cdl.OrphanRecordExportWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.export.ExportData;
import com.latticeengines.serviceflows.workflow.export.ExportSegmentExportToS3;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component(OrphanRecordExportWorkflowConfiguration.WORK_FLOW_NAME)
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class OrphanRecordExportWorkflow extends AbstractWorkflow<OrphanRecordExportWorkflowConfiguration> {

    @Autowired
    private OrphanRecordExportStep orphanRecordExportStep;

    @Autowired
    private ExportData exportData;

    @Autowired
    private ExportSegmentExportToS3 exportSegmentExportToS3;

    @Autowired
    private SegmentExportListener segmentExportListener;

    @Override
    public Workflow defineWorkflow(OrphanRecordExportWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config)
                .next(orphanRecordExportStep)
                .next(exportData)
                .next(exportSegmentExportToS3)
                .listener(segmentExportListener)
                .build();
    }

}
