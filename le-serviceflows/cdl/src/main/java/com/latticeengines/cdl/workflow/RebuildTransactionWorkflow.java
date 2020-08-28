package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.AggDailyTransactionStep;
import com.latticeengines.cdl.workflow.steps.AggPeriodTransactionStep;
import com.latticeengines.cdl.workflow.steps.BuildDailyTransaction;
import com.latticeengines.cdl.workflow.steps.BuildPeriodTransaction;
import com.latticeengines.cdl.workflow.steps.RollupProductStepWrapper;
import com.latticeengines.cdl.workflow.steps.SplitTransactionStep;
import com.latticeengines.cdl.workflow.steps.rebuild.ProfilePurchaseHistoryWrapper;
import com.latticeengines.cdl.workflow.steps.rebuild.ProfileTransactionWrapper;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.RebuildTransactionWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RebuildTransactionWorkflow extends AbstractWorkflow<RebuildTransactionWorkflowConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(RebuildTransactionWorkflow.class);

    @Value("${cdl.txn.use.legacy:false}")
    private boolean useLegacyTransactionSteps;

    @Inject
    private ProfileTransactionWrapper profileTransactionWrapper;

    @Inject
    private ProfilePurchaseHistoryWrapper profilePurchaseHistoryWrapper;

    @Inject
    private RollupProductStepWrapper rollupProductStepWrapper;

    @Inject
    private SplitTransactionStep splitTransactionStep;

    @Inject
    private AggDailyTransactionStep aggDailyTransactionStep;

    @Inject
    private AggPeriodTransactionStep aggPeriodTransactionStep;

    @Inject
    private BuildDailyTransaction buildDailyTransaction;

    @Inject
    private BuildPeriodTransaction buildPeriodTransaction;

    @Override
    public Workflow defineWorkflow(RebuildTransactionWorkflowConfiguration config) {
        log.info("Using legacy steps: {}", useLegacyTransactionSteps);
        if (useLegacyTransactionSteps) {
            return new WorkflowBuilder(name(), config) //
                    .next(profileTransactionWrapper) //
                    .next(profilePurchaseHistoryWrapper) //
                    .build();
        }
        return new WorkflowBuilder(name(), config) //
                .next(rollupProductStepWrapper) //
                .next(splitTransactionStep) //
                .next(aggDailyTransactionStep) //
                .next(aggPeriodTransactionStep) //
                .next(buildDailyTransaction) //
                .next(buildPeriodTransaction) //
                .next(profilePurchaseHistoryWrapper) //
                .build();
    }
}
