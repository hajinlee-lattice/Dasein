package com.latticeengines.dcp.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.dcp.workflow.listeners.DataReportListener;
import com.latticeengines.dcp.workflow.steps.RollupDataReport;
import com.latticeengines.domain.exposed.serviceflows.dcp.DCPDataReportWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("dcpDataReportWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class DCPDataReportWorkflow extends AbstractWorkflow<DCPDataReportWorkflowConfiguration> {

    @Inject
    private RollupDataReport rollupDataReport;

    @Inject
    private DataReportListener dataReportListener;

    @Override
    public Workflow defineWorkflow(DCPDataReportWorkflowConfiguration workflowConfig) {
        return new WorkflowBuilder(name(), workflowConfig)
                .next(rollupDataReport)
                .listener(dataReportListener)
                .build();
    }
}
