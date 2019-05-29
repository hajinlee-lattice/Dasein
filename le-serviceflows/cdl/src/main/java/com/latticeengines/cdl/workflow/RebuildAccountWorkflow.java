package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.rebuild.EnrichAccountWrapper;
import com.latticeengines.cdl.workflow.steps.rebuild.FilterAccountExport;
import com.latticeengines.cdl.workflow.steps.rebuild.FilterAccountFeature;
import com.latticeengines.cdl.workflow.steps.rebuild.GenerateBucketedAccountWrapper;
import com.latticeengines.cdl.workflow.steps.rebuild.ProfileAccountWrapper;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.RebuildAccountWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("rebuildAccountWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RebuildAccountWorkflow extends AbstractWorkflow<RebuildAccountWorkflowConfiguration> {

    @Inject
    private EnrichAccountWrapper enrichAccount;

    @Inject
    private ProfileAccountWrapper profileAccount;

    @Inject
    private FilterAccountFeature filterAccountFeature;

    @Inject
    private FilterAccountExport filterAccountExport;

    @Inject
    private GenerateBucketedAccountWrapper generateBucketedAccount;

    @Override
    public Workflow defineWorkflow(RebuildAccountWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(enrichAccount) //
                .next(profileAccount) //
                .next(filterAccountFeature) //
                .next(filterAccountExport) //
                .next(generateBucketedAccount) //
                .build();
    }
}
