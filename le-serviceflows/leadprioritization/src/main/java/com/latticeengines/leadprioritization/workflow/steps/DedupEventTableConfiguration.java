package com.latticeengines.leadprioritization.workflow.steps;

import com.latticeengines.serviceflows.workflow.dataflow.DataFlowStepConfiguration;

public class DedupEventTableConfiguration extends DataFlowStepConfiguration {
    public DedupEventTableConfiguration() {
        setPurgeSources(true);
    }
}
