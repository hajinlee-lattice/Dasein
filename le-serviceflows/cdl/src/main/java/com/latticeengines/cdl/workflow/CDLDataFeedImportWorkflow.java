package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.listeners.DataFeedTaskImportListener;
import com.latticeengines.cdl.workflow.steps.importdata.ImportDataFeedTask;
import com.latticeengines.cdl.workflow.steps.importdata.ImportDataReport;
import com.latticeengines.cdl.workflow.steps.importdata.ImportDataTableFromS3;
import com.latticeengines.cdl.workflow.steps.importdata.PrepareImport;
import com.latticeengines.cdl.workflow.steps.validations.InputFileValidator;
import com.latticeengines.cdl.workflow.steps.validations.ValidateProductSpark;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.CDLDataFeedImportWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.export.ExportDataFeedImportToS3;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("cdlDataFeedImportWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CDLDataFeedImportWorkflow extends AbstractWorkflow<CDLDataFeedImportWorkflowConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(CDLDataFeedImportWorkflow.class);
    @Value("${cdl.merge.product.use.spark}")
    private boolean useMergeProductSpark;

    @Inject
    private PrepareImport prepareImport;

    @Inject
    private ImportDataTableFromS3 importDataTableFromS3;

    @Inject
    private ImportDataFeedTask importDataFeedTask;

    @Inject
    private InputFileValidator inputFileValidator;

    @Inject
    private ValidateProductSpark validateProductSpark;

    @Inject
    private ImportDataReport importDataReport;

    @Inject
    private DataFeedTaskImportListener dataFeedTaskImportListener;

    @Inject
    private ExportDataFeedImportToS3 exportDataFeedImportToS3;

    @Override
    public Workflow defineWorkflow(CDLDataFeedImportWorkflowConfiguration config) {
        WorkflowBuilder builder = new WorkflowBuilder(name(), config)//
                .next(prepareImport)
                .next(importDataTableFromS3)//
                .next(importDataFeedTask);
        log.info("product is " + config.getEntity() + " use merge product spark " + useMergeProductSpark);
        if (BusinessEntity.Product.equals(config.getEntity()) && useMergeProductSpark) {
            builder = builder.next(validateProductSpark).next(importDataReport);
        } else {
            builder = builder.next(inputFileValidator);
        }
        return builder
                .next(exportDataFeedImportToS3)//
                .listener(dataFeedTaskImportListener)//
                .build();
    }
}
