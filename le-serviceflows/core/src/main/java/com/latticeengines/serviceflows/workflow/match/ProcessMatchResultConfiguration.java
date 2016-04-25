package com.latticeengines.serviceflows.workflow.match;

import com.latticeengines.serviceflows.workflow.dataflow.DataFlowStepConfiguration;

public class ProcessMatchResultConfiguration extends DataFlowStepConfiguration {

    public ProcessMatchResultConfiguration() {
        setPurgeSources(false);
        setBeanName("parseMatchResult");
    }

}
