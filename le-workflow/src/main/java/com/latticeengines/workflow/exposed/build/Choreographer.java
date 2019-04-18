package com.latticeengines.workflow.exposed.build;

import java.util.List;

import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public interface Choreographer {

    void linkStepNamespaces(List<String> stepNamespaces);

    boolean skipStep(AbstractStep<? extends BaseStepConfiguration> step, int seq);

    Choreographer DEFAULT_CHOREOGRAPHER = new BaseChoreographer();

}
