package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.listeners.EntityExportWorkflowListener;
import com.latticeengines.cdl.workflow.steps.export.ExtractAtlasEntity;
import com.latticeengines.cdl.workflow.steps.export.SaveAtlasExportCSV;
import com.latticeengines.domain.exposed.serviceflows.cdl.EntityExportWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;


@Component("entityExportWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class EntityExportWorkflow extends AbstractWorkflow<EntityExportWorkflowConfiguration> {

    @Inject
    private ExtractAtlasEntity extractAtlasEntity;

    @Inject
    private SaveAtlasExportCSV saveAtlasExportCSV;

    @Inject
    private EntityExportWorkflowListener listener;

    @Override
    public Workflow defineWorkflow(EntityExportWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(extractAtlasEntity) //
                .next(saveAtlasExportCSV) //
                .listener(listener) //
                .build();

    }

}
