package com.latticeengines.cdl.workflow.choreographers;

import static com.latticeengines.workflow.exposed.build.BaseWorkflowStep.CHOREOGRAPHER_CONTEXT_KEY;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.springframework.batch.item.ExecutionContext;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cdl.ChoreographerContext;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractStep;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

public class ProcessAccountChoreographerUnitTestNG {

    @Test(groups = "unit")
    public void checkManyUpdate() {
        ProcessAccountChoreographer choreographer = new ProcessAccountChoreographer();
        AbstractStep<ProcessAccountStepConfiguration> step = new AbstractStep<ProcessAccountStepConfiguration>() {
            @Override
            public void execute() {
            }
        };
        step.setExecutionContext(new ExecutionContext());
        choreographer.checkManyUpdate(step);
        Assert.assertEquals(choreographer.hasManyUpdate, false);
        Map<BusinessEntity, Long> updateValueMap = new HashMap<>();
        updateValueMap.put(BusinessEntity.Account, 2L);
        step.putObjectInContext(BaseWorkflowStep.UPDATED_RECORDS, updateValueMap);

        Map<BusinessEntity, Long> existValueMap = new HashMap<>();
        existValueMap.put(BusinessEntity.Account, 10L);
        step.putObjectInContext(BaseWorkflowStep.EXISTING_RECORDS, existValueMap);

        choreographer.checkManyUpdate(step);
        Assert.assertEquals(choreographer.hasManyUpdate, false);

        updateValueMap.put(BusinessEntity.Account, 5L);
        step.putObjectInContext(BaseWorkflowStep.UPDATED_RECORDS, updateValueMap);
        choreographer.checkManyUpdate(step);
        Assert.assertEquals(choreographer.hasManyUpdate, true);

    }

    @Test(groups = "unit")
    public void checkDataCloudChange() {
        ProcessAccountChoreographer choreographer = new ProcessAccountChoreographer();
        AbstractStep<ProcessAccountStepConfiguration> step = new AbstractStep<ProcessAccountStepConfiguration>() {
            @Override
            public void execute() {
            }
        };
        step.setExecutionContext(new ExecutionContext());
        ChoreographerContext grapherContext = new ChoreographerContext();
        grapherContext.setDataCloudChanged(true);
        step.putObjectInContext(CHOREOGRAPHER_CONTEXT_KEY, grapherContext);
        choreographer.checkDataCloudChange(step);
        Assert.assertEquals(choreographer.dataCloudChanged, true);
    }

    @Test(groups = "unit")
    public void checkJobImpactedEntity() {
        ProcessAccountChoreographer choreographer = new ProcessAccountChoreographer();
        AbstractStep<ProcessAccountStepConfiguration> step = new AbstractStep<ProcessAccountStepConfiguration>() {
            @Override
            public void execute() {
            }
        };
        step.setExecutionContext(new ExecutionContext());
        ChoreographerContext grapherContext = new ChoreographerContext();
        grapherContext.setJobImpactedEntities(new HashSet<>(Collections.singletonList(BusinessEntity.Contact)));
        step.putObjectInContext(CHOREOGRAPHER_CONTEXT_KEY, grapherContext);
        choreographer.checkJobImpactedEntity(step);
        Assert.assertEquals(choreographer.jobImpacted, false);

        grapherContext.setJobImpactedEntities(
                new HashSet<>(Arrays.asList(BusinessEntity.Account, BusinessEntity.Contact)));
        step.putObjectInContext(CHOREOGRAPHER_CONTEXT_KEY, grapherContext);
        choreographer.checkJobImpactedEntity(step);
        Assert.assertEquals(choreographer.jobImpacted, true);
    }

}
