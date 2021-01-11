package com.latticeengines.workflow.service.impl;

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.workflow.exposed.service.WorkflowService;
import com.latticeengines.workflow.functionalframework.DynamicWorkflowChoreographer;
import com.latticeengines.workflow.functionalframework.DynamicWorkflowConfiguration;
import com.latticeengines.workflow.functionalframework.NamedStep;
import com.latticeengines.workflow.functionalframework.WorkflowTestNGBase;

public class DynamicWorkflowTestNG extends WorkflowTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DynamicWorkflowTestNG.class);

    @Inject
    private WorkflowService workflowService;

    @Inject
    private TenantService tenantService;

    @Inject
    private DynamicWorkflowChoreographer choreographer;

    @Resource(name = "stepA")
    private NamedStep stepA;

    @Resource(name = "stepB")
    private NamedStep stepB;

    @Resource(name = "stepC")
    private NamedStep stepC;

    @Resource(name = "stepD")
    private NamedStep stepD;

    @Resource(name = "stepSkippedOnMissingConfig")
    private NamedStep stepSkippedOnMissingConfig;

    private WorkflowConfiguration workflowConfig;

    private String customerSpace;

    private WorkflowExecutionId workflowId;

    @BeforeClass(groups = "functional")
    public void setup() {
        customerSpace = bootstrapWorkFlowTenant().toString();
        workflowConfig = new DynamicWorkflowConfiguration.Builder().build();
        workflowConfig.setCustomerSpace(CustomerSpace.parse(customerSpace));
        workflowService.registerJob(workflowConfig, applicationContext);
    }

    @AfterClass(groups = "functional")
    public void cleanup() {
        Tenant tenant = tenantService.findByTenantId(customerSpace);
        if (tenant != null) {
            tenantService.discardTenant(tenant);
        }

        super.cleanup(workflowId.getId());
    }

    @Test(groups = "functional", dataProvider = "dynamicWorkflowTestConfig")
    public void testDynamicWorkflow(String skipStrategy, List<Integer> stepsShouldBeSkipped) throws Exception {
        choreographer.examinedSteps = new ArrayList<>();
        choreographer.setSkipStrategy(skipStrategy);

        workflowId = workflowService.start(workflowConfig);
        BatchStatus status = workflowService.waitForCompletion(workflowId, MAX_MILLIS_TO_WAIT, 1000).getStatus();
        assertEquals(status, BatchStatus.COMPLETED);
        verifySteps(stepsShouldBeSkipped);
    }

    @DataProvider(name = "dynamicWorkflowTestConfig")
    private Object[][] dynamicWorkflowTestConfig() {
        return new Object[][] { { DynamicWorkflowChoreographer.NO_SKIP, Collections.emptyList() }, //
                { DynamicWorkflowChoreographer.SKIP_ALL_C, Arrays.asList(4, 6, 7) }, //
                { DynamicWorkflowChoreographer.SKIP_ROOT_A, Collections.singletonList(0) }, //
                { DynamicWorkflowChoreographer.SKIP_C_IN_WORKFLOW_B, Arrays.asList(4, 7) }, //
                { DynamicWorkflowChoreographer.SKIP_ROOT_WORKFLOW_A, Arrays.asList(2, 3, 4, 5) }, //
                { DynamicWorkflowChoreographer.SKIP_ROOT_WORKFLOW_B, Arrays.asList(7, 8) }, //
                { DynamicWorkflowChoreographer.SKIP_ALL_WORKFLOW_B, Arrays.asList(4, 5, 7, 8) }, //
        };
    }

    private void verifySteps(Collection<Integer> skippedSteps) {
        log.info("Examined steps: " + JsonUtils.pprint(choreographer.examinedSteps));
        int idx = 0;
        List<List<Object>> shouldSkip = new ArrayList<>();
        List<List<Object>> shouldNotSkip = new ArrayList<>();

        for (List<Object> objs : choreographer.examinedSteps) {
            int stepSeq = (int) objs.get(0);
            String stepName = (String) objs.get(1);
            boolean skipped = (boolean) objs.get(2);

            Assert.assertEquals(objs.get(0), idx);

            if (skippedSteps.contains(stepSeq) && !skipped) {
                shouldSkip.add(objs);
            }
            if (!skippedSteps.contains(stepSeq) && skipped) {
                shouldNotSkip.add(objs);
            }

            switch (stepSeq) {
            case 0:
            case 2:
                Assert.assertEquals(stepName, stepA.getStepName());
                break;
            case 1:
            case 3:
                Assert.assertEquals(stepName, stepB.getStepName());
                break;
            case 4:
            case 6:
            case 7:
                Assert.assertEquals(stepName, stepC.getStepName());
                break;
            case 5:
            case 8:
            case 9:
                Assert.assertEquals(stepName, stepD.getStepName());
                break;
            case 10:
                Assert.assertEquals(stepName, stepSkippedOnMissingConfig.getStepName());
                break;
            default:
                Assert.fail("There should not be a " + stepSeq + "-th step");
            }
            idx++;
        }

        if (CollectionUtils.isNotEmpty(shouldSkip) || CollectionUtils.isNotEmpty(shouldNotSkip)) {
            String errorMsg = "";
            List<String> tokens = shouldSkip.stream().map(list -> {
                int stepSeq = (int) list.get(0);
                String stepName = (String) list.get(1);
                return String.format("[%d] %s", stepSeq, stepName);
            }).collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(tokens)) {
                errorMsg += "These steps are expected to be skipped: " + StringUtils.join(tokens, ", ") + ". ";
            }
            tokens = shouldNotSkip.stream().map(list -> {
                int stepSeq = (int) list.get(0);
                String stepName = (String) list.get(1);
                return String.format("[%d] %s", stepSeq, stepName);
            }).collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(tokens)) {
                errorMsg += "These steps are expected not to be skipped: " + StringUtils.join(tokens, ", ") + ".";
            }
            Assert.fail(errorMsg);
        }
    }

}
