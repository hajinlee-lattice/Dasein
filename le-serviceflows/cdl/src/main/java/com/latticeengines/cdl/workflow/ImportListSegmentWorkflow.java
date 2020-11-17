package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.listeners.ImportListSegmentListener;
import com.latticeengines.cdl.workflow.steps.importdata.CopyListSegmentCSV;
import com.latticeengines.cdl.workflow.steps.importdata.ExportListSegmentCSVToS3;
import com.latticeengines.cdl.workflow.steps.importdata.ExtractListSegmentCSV;
import com.latticeengines.domain.exposed.serviceflows.cdl.ImportListSegmentWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("importListSegmentWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ImportListSegmentWorkflow extends AbstractWorkflow<ImportListSegmentWorkflowConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ImportListSegmentWorkflow.class);

    @Inject
    private CopyListSegmentCSV copyListSegmentCSV;

    @Inject
    private ExtractListSegmentCSV extractListSegmentCSV;

    @Inject
    private ExportListSegmentCSVToS3 exportListSegmentCSVToS3;

    @Inject
    private ImportListSegmentListener importListSegmentListener;

    @Override
    public Workflow defineWorkflow(ImportListSegmentWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config)
                .next(copyListSegmentCSV)
                .next(extractListSegmentCSV)
                .next(exportListSegmentCSVToS3)
                .listener(importListSegmentListener)
                .build();
    }
}
