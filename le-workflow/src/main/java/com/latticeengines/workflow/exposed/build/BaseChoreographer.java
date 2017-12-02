package com.latticeengines.workflow.exposed.build;

import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public class BaseChoreographer implements Choreographer {

    protected static final String ROOT = "root";

    private List<String> stepDAG;

    @Override
    public boolean skipStep(AbstractStep<? extends BaseStepConfiguration> step, int seq) {
        return step.getConfiguration() != null && step.getConfiguration().isSkipStep();
    }

    @Override
    public void linkStepDAG(List<String> stepDAG) {
        this.stepDAG = stepDAG;
    }

    protected String getStepNamespace(int seq) {
        try {
            String namespace = stepDAG.get(seq);
            return namespace == null ? "" : namespace;
        } catch (IndexOutOfBoundsException e) {
            return null;
        }
    }

    protected String getParentWorkflow(int seq) {
        String namespace = getStepNamespace(seq);
        if (StringUtils.isBlank(namespace)) {
            return ROOT;
        } else {
            return namespace.substring(namespace.lastIndexOf(".") + 1);
        }
    }

}
