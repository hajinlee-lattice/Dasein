package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.listeners.DataFeedTaskImportListener;
import com.latticeengines.cdl.workflow.steps.importdata.ImportDataFeedTask;
import com.latticeengines.cdl.workflow.steps.importdata.ImportDataTableFromS3;
import com.latticeengines.cdl.workflow.steps.importdata.PrepareImport;
import com.latticeengines.cdl.workflow.steps.validations.InputFileValidator;
import com.latticeengines.domain.exposed.serviceflows.cdl.CDLDataFeedImportWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.export.ExportDataFeedImportToS3;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("cdlDataFeedImportWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CDLDataFeedImportWorkflow extends AbstractWorkflow<CDLDataFeedImportWorkflowConfiguration> {

    @Inject
    private PrepareImport prepareImport;

    @Inject
    private ImportDataTableFromS3 importDataTableFromS3;

    @Inject
    private ImportDataFeedTask importDataFeedTask;

    @Inject
    private InputFileValidator inputFileValidator;

    @Inject
    private DataFeedTaskImportListener dataFeedTaskImportListener;

    @Inject
    private ExportDataFeedImportToS3 exportDataFeedImportToS3;

    @Override
    public Workflow defineWorkflow(CDLDataFeedImportWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config)//
                .next(prepareImport)
                .next(importDataTableFromS3)//
                .next(importDataFeedTask)//
                .next(inputFileValidator)//
                .next(exportDataFeedImportToS3)//
                .listener(dataFeedTaskImportListener)//
                .build();
    }
}
