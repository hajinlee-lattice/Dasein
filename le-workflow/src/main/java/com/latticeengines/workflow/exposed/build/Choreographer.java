package com.latticeengines.workflow.exposed.build;

import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

import java.util.List;

public interface Choreographer {

    void linkStepNamespaces(List<List<String>> stepNamespaces);
    boolean skipStep(final AbstractStep<? extends BaseStepConfiguration> step, int seq);

    Choreographer DEFAULT_CHOREOGRAPHER = new BaseChoreographer();

}
