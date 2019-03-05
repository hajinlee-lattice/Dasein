package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.rebuild.ProfileProductHierarchyWrapper;
import com.latticeengines.cdl.workflow.steps.rebuild.ProfileProductWrapper;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.RebuildProductWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("rebuildProductWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RebuildProductWorkflow extends AbstractWorkflow<RebuildProductWorkflowConfiguration> {

    @Inject
    private ProfileProductWrapper profileProductWrapper;

    @Inject
    private ProfileProductHierarchyWrapper profileProductHierarchyWrapper;

    @Override
    public Workflow defineWorkflow(RebuildProductWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(profileProductWrapper) //
                .next(profileProductHierarchyWrapper) //
                .build();
    }
}
