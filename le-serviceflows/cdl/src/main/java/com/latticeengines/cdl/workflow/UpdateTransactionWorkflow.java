package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.AggDailyTransactionStep;
import com.latticeengines.cdl.workflow.steps.AggPeriodTransactionStep;
import com.latticeengines.cdl.workflow.steps.BuildDailyTransaction;
import com.latticeengines.cdl.workflow.steps.BuildPeriodTransaction;
import com.latticeengines.cdl.workflow.steps.BuildSpendingAnalysis;
import com.latticeengines.cdl.workflow.steps.RollupProductStepWrapper;
import com.latticeengines.cdl.workflow.steps.SplitTransactionStep;
import com.latticeengines.cdl.workflow.steps.update.ClonePurchaseHistory;
import com.latticeengines.cdl.workflow.steps.update.CloneTransaction;
import com.latticeengines.cdl.workflow.steps.update.MergePeriodTransactionDiff;
import com.latticeengines.cdl.workflow.steps.update.MergeTransactionDiff;
import com.latticeengines.cdl.workflow.steps.update.ProcessTransactionDiffWrapper;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.UpdateTransactionWorkflowConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("updateTransactionWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class UpdateTransactionWorkflow extends AbstractWorkflow<UpdateTransactionWorkflowConfiguration> {

    @Inject
    private CloneTransaction cloneTransaction;

    @Inject
    private ClonePurchaseHistory clonePurchaseHistory;

    @Inject
    private ProcessTransactionDiffWrapper processTransactionDiffWrapper;

    @Inject
    private MergeTransactionDiff mergeTransactionDiff;

    @Inject
    private MergePeriodTransactionDiff mergePeriodTransactionDiff;

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

    @Inject
    private BuildSpendingAnalysis buildSpendingAnalysis;

    @Value("${cdl.txn.use.legacy:false}")
    private boolean useLegacyTransactionSteps;

    @Override
    public Workflow defineWorkflow(UpdateTransactionWorkflowConfiguration config) {
        if (useLegacyTransactionSteps) {
            return new WorkflowBuilder(name(), config) //
                    .next(cloneTransaction) //
                    .next(clonePurchaseHistory) //
                    .next(processTransactionDiffWrapper) //
                    .next(mergeTransactionDiff) //
                    .next(mergePeriodTransactionDiff) //
                    .build();
        }
        return new WorkflowBuilder(name(), config) //
                .next(cloneTransaction) //
                .next(clonePurchaseHistory) //
                // --- shared with rebuild transaction ---
                .next(rollupProductStepWrapper) //
                .next(splitTransactionStep) //
                .next(aggDailyTransactionStep) //
                .next(aggPeriodTransactionStep) //
                .next(buildDailyTransaction) //
                .next(buildPeriodTransaction) //
                .next(buildSpendingAnalysis) //
                // --- shared with rebuild transaction ---
                .build();
    }
}
