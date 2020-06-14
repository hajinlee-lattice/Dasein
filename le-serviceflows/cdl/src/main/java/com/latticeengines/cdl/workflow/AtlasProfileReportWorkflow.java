package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.report.GenerateProfileReport;
import com.latticeengines.domain.exposed.serviceflows.cdl.AtlasProfileReportWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Lazy
@Component(AtlasProfileReportWorkflowConfiguration.WORKFLOW_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class AtlasProfileReportWorkflow extends AbstractWorkflow<AtlasProfileReportWorkflowConfiguration> {

    @Inject
    private GenerateProfileReport generateProfileReport;

    @Override
    public Workflow defineWorkflow(AtlasProfileReportWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config).next(generateProfileReport).build();
    }

}
