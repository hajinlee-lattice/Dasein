package com.latticeengines.workflow.functionalframework;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableList;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractStep;
import com.latticeengines.workflow.exposed.build.BaseChoreographer;
import com.latticeengines.workflow.exposed.build.Choreographer;

@Component("dynamicWorkflowChoreographer")
public class DynamicWorkflowChoreographer extends BaseChoreographer implements Choreographer {

    public static final String NO_SKIP = "NO_SKIP";
    public static final String SKIP_ALL_C = "SKIP_ALL_C";
    public static final String SKIP_ROOT_A = "SKIP_ROOT_A";
    public static final String SKIP_C_IN_WORKFLOW_B = "SKIP_C_IN_WORKFLOW_B";
    public static final String SKIP_ROOT_WORKFLOW_A = "SKIP_ROOT_WORKFLOW_A";
    public static final String SKIP_ROOT_WORKFLOW_B = "SKIP_ROOT_WORKFLOW_B";
    public static final String SKIP_ALL_WORKFLOW_B = "SKIP_ALL_WORKFLOW_B";

    public List<List<Object>> examinedSteps = new ArrayList<>();
    private String skipStrategy;

    @Resource(name = "stepA")
    private NamedStep stepA;

    @Resource(name = "stepC")
    private NamedStep stepC;

    @Inject
    private DynamicSubWorkflowA workflowA;

    @Inject
    private DynamicSubWorkflowB workflowB;

    @Override
    public boolean skipStep(AbstractStep<? extends BaseStepConfiguration> step, int seq) {
        boolean skip = false;
        NamedStep namedStep = (NamedStep) step;
        switch (skipStrategy) {
            case SKIP_ALL_C:
                skip = skipAllC(namedStep);
                break;
            case SKIP_ROOT_A:
                skip = skipRootA(namedStep, seq);
                break;
            case SKIP_C_IN_WORKFLOW_B:
                skip = skipCInWorkflowB(namedStep, seq);
                break;
            case SKIP_ROOT_WORKFLOW_A:
                skip = skipRootWorkflow(seq, workflowA.name());
                break;
            case SKIP_ROOT_WORKFLOW_B:
                skip = skipRootWorkflow(seq, workflowB.name());
                break;
            case SKIP_ALL_WORKFLOW_B:
                skip = skipWorkflow(seq, workflowB.name());
                break;
            case NO_SKIP:
            default:
                break;
        }

        examinedSteps.add(ImmutableList.of(seq, namedStep.getStepName(), skip));
        return skip;
    }

    private boolean skipAllC(NamedStep namedStep) {
        return stepC.getStepName().equals(namedStep.getStepName());
    }

    private boolean skipRootA(NamedStep namedStep, int seq) {
        if (stepA.getStepName().equals(namedStep.getStepName())) {
            List<String> namespace = getStepNamespace(seq);
            if (namespace.size() == 0) {
                return true;
            }
        }
        return false;
    }

    private boolean skipCInWorkflowB(NamedStep namedStep, int seq) {
        return  namespaceEndWith(seq, Collections.singletonList(workflowB.name())) //
                && stepC.getStepName().equals(namedStep.getStepName());
    }

    private boolean skipRootWorkflow(int seq, String workflowName) {
        return namespaceBeginWith(seq, Collections.singletonList(workflowName));
    }

    private boolean skipWorkflow(int seq, String workflowName) {
        return namespaceContains(seq, Collections.singletonList(workflowName));
    }

    public void setSkipStrategy(String skipStrategy) {
        this.skipStrategy = skipStrategy;
    }
}
