package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.listeners.OrphanRecordsExportListener;
import com.latticeengines.cdl.workflow.steps.ComputeOrphanRecordsStep;
import com.latticeengines.domain.exposed.serviceflows.cdl.OrphanRecordsExportWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.export.ExportData;
import com.latticeengines.serviceflows.workflow.export.ExportOrphansRecordsToS3;
import com.latticeengines.serviceflows.workflow.export.ImportTablesForOrphanReportFromS3;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component(OrphanRecordsExportWorkflowConfiguration.WORKFLOW_NAME)
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class OrphanRecordsExportWorkflow extends AbstractWorkflow<OrphanRecordsExportWorkflowConfiguration> {
    @Inject
    private ImportTablesForOrphanReportFromS3 importTablesForOrphanReportFromS3;

    @Inject
    private ComputeOrphanRecordsStep computeOrphanRecords;

    @Inject
    private ExportData exportData;

    @Inject
    private ExportOrphansRecordsToS3 exportOrphansRecordsToS3;

    @Inject
    private OrphanRecordsExportListener orphanRecordsListener;

    @Override
    public Workflow defineWorkflow(OrphanRecordsExportWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config)
                .next(importTablesForOrphanReportFromS3)
                .next(computeOrphanRecords)
                .next(exportData)
                .next(exportOrphansRecordsToS3)
                .listener(orphanRecordsListener)
                .build();
    }
}
