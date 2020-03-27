package com.latticeengines.dcp.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.dcp.workflow.listeners.SourceImportListener;
import com.latticeengines.dcp.workflow.steps.FinishImportSource;
import com.latticeengines.dcp.workflow.steps.ImportSource;
import com.latticeengines.dcp.workflow.steps.SplitImportMatchResult;
import com.latticeengines.dcp.workflow.steps.StartImportSource;
import com.latticeengines.dcp.workflow.steps.export.ExportSourceImportToS3;
import com.latticeengines.domain.exposed.serviceflows.dcp.DCPSourceImportWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.match.MatchDataCloudWorkflow;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("dcpSourceImportWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class DCPSourceImportWorkflow extends AbstractWorkflow<DCPSourceImportWorkflowConfiguration> {

    @Inject
    private StartImportSource start;

    @Inject
    private ImportSource importSource;

    @Inject
    private MatchDataCloudWorkflow match;

    @Inject
    private SplitImportMatchResult splitMatchResult;

    @Inject
    private ExportSourceImportToS3 exportSourceImportToS3;

    @Inject
    private FinishImportSource finish;

    @Inject
    private SourceImportListener sourceImportListener;

    @Override
    public Workflow defineWorkflow(DCPSourceImportWorkflowConfiguration workflowConfig) {
        return new WorkflowBuilder(name(), workflowConfig)
                .next(start)
                .next(importSource)
                .next(match)
                .next(splitMatchResult)
                .next(exportSourceImportToS3)
                .next(finish)
                .listener(sourceImportListener)
                .build();
    }
}
