package com.latticeengines.cdl.workflow.choreographers;

import org.springframework.batch.item.ExecutionContext;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cdl.ChoreographerContext;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractStep;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

public class ProcessTransactionChoreographerUnitTestNG {
    @Test(groups = "unit")
    public void checkRebuildCausedByReset() {
        ProcessTransactionChoreographer choreographer = initProcessTransactionChoreographer();
        Assert.assertFalse(choreographer.shouldRebuild());

        choreographer.reset = true;
        Assert.assertFalse(choreographer.shouldRebuild());
    }

    @Test(groups = "unit")
    public void checkRebuildCausedByProducts() {
        ProcessTransactionChoreographer choreographer = initProcessTransactionChoreographer();
        Assert.assertFalse(choreographer.shouldRebuild());

        choreographer.setHasRawStore(true);
        choreographer.setHasProducts(true);
        choreographer.setHasProductChange(false);
        Assert.assertFalse(choreographer.shouldRebuild());

        choreographer.setHasRawStore(false);
        choreographer.setHasProducts(true);
        choreographer.setHasProductChange(true);
        Assert.assertFalse(choreographer.shouldRebuild());

        choreographer.setHasRawStore(true);
        choreographer.setHasProducts(false);
        choreographer.setHasProductChange(true);
        Assert.assertFalse(choreographer.shouldRebuild());

        choreographer.setHasRawStore(true);
        choreographer.setHasProducts(true);
        choreographer.setHasProductChange(true);
        Assert.assertTrue(choreographer.shouldRebuild());
    }

    @Test(groups = "unit")
    public void checkRebuildCausedByBusinessCalendarChanged() {
        ProcessTransactionChoreographer choreographer = initProcessTransactionChoreographer();
        Assert.assertFalse(choreographer.shouldRebuild());

        AbstractStep<ProcessTransactionStepConfiguration> step =
                new AbstractStep<ProcessTransactionStepConfiguration>() {
                    @Override
                    public void execute() {
                        // do nothing
                    }
                };
        step.setConfiguration(new ProcessTransactionStepConfiguration());
        step.setExecutionContext(new ExecutionContext());
        ChoreographerContext context = new ChoreographerContext();
        context.setBusinessCalenderChanged(false);
        step.putObjectInContext(BaseWorkflowStep.CHOREOGRAPHER_CONTEXT_KEY, context);
        choreographer.checkBusinessCalendarChanged(step);
        Assert.assertFalse(choreographer.shouldRebuild());

        context.setBusinessCalenderChanged(true);
        step.putObjectInContext(BaseWorkflowStep.CHOREOGRAPHER_CONTEXT_KEY, context);
        choreographer.checkBusinessCalendarChanged(step);
        Assert.assertTrue(choreographer.shouldRebuild());
    }

    private ProcessTransactionChoreographer initProcessTransactionChoreographer() {
        ProcessTransactionChoreographer choreographer = new ProcessTransactionChoreographer();
        choreographer.setProductChoreographer(new ProcessProductChoreographer());
        return choreographer;
    }
}
