package com.latticeengines.leadprioritization.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.leadprioritization.ImportVdbTableAndPublishWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.importvdbtable.ImportVdbTable;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("importVdbTableAndPublishWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ImportVdbTableAndPublishWorkflow extends AbstractWorkflow<ImportVdbTableAndPublishWorkflowConfiguration> {

    @Inject
    private ImportVdbTable importVdbTable;

    @Override
    public Workflow defineWorkflow(ImportVdbTableAndPublishWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config).next(importVdbTable).build();
    }
}
