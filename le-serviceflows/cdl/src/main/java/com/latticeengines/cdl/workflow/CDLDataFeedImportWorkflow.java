package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.listeners.DataFeedTaskImportListener;
import com.latticeengines.cdl.workflow.steps.importdata.ImportDataFeedTask;
import com.latticeengines.domain.exposed.serviceflows.cdl.CDLDataFeedImportWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.export.ExportDataFeedImportToS3;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("cdlDataFeedImportWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CDLDataFeedImportWorkflow extends AbstractWorkflow<CDLDataFeedImportWorkflowConfiguration> {

    @Autowired
    private ImportDataFeedTask importDataFeedTask;

    @Autowired
    private DataFeedTaskImportListener dataFeedTaskImportListener;

    @Inject
    private ExportDataFeedImportToS3 exportDataFeedImportToS3;

    @Override
    public Workflow defineWorkflow(CDLDataFeedImportWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config)//
                .next(importDataFeedTask)//
                .next(exportDataFeedImportToS3)//
                .listener(dataFeedTaskImportListener)//
                .build();
    }
}
