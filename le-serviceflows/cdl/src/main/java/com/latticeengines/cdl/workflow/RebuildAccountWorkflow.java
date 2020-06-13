package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.rebuild.EnrichAccountWrapper;
import com.latticeengines.cdl.workflow.steps.rebuild.EnrichLatticeAccount;
import com.latticeengines.cdl.workflow.steps.rebuild.GenerateBucketedAccountWrapper;
import com.latticeengines.cdl.workflow.steps.rebuild.ProfileAccountWrapper;
import com.latticeengines.cdl.workflow.steps.rebuild.SplitAccountStores;
import com.latticeengines.cdl.workflow.steps.rebuild.UpdateAccountExport;
import com.latticeengines.cdl.workflow.steps.rebuild.UpdateAccountFeatures;
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
    private EnrichLatticeAccount enrichLatticeAccount;

    @Inject
    private ProfileAccountWrapper profileAccount;

    @Inject
    private SplitAccountStores splitAccountStores;

    @Inject
    private UpdateAccountExport updateAccountExport;

    @Inject
    private UpdateAccountFeatures updateAccountFeatures;

    @Inject
    private GenerateBucketedAccountWrapper generateBucketedAccount;

    @Value("${cdl.use.changelist}")
    private boolean useChangeList;

    @Override
    public Workflow defineWorkflow(RebuildAccountWorkflowConfiguration config) {
        WorkflowBuilder builder = new WorkflowBuilder(name(), config);
        if (useChangeList) {
            builder = builder.next(enrichLatticeAccount).next(updateAccountExport).next(updateAccountFeatures);
        } else {
            builder = builder.next(enrichAccount).next(splitAccountStores);
        }
        return builder //
                .next(profileAccount) //
                .next(generateBucketedAccount) //
                .build();
    }
}
