package com.latticeengines.serviceflows.workflow.export;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("exportWorkflow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExportWorkflow extends AbstractWorkflow<WorkflowConfiguration> {

    @Inject
    private ExportData exportData;

    @Override
    public Workflow defineWorkflow(WorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config)//
                .next(exportData) //
                .build();
    }
}
