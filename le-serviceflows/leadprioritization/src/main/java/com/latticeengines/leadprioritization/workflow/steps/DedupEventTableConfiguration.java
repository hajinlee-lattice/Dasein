package com.latticeengines.leadprioritization.workflow.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.serviceflows.workflow.dataflow.DataFlowStepConfiguration;

public class DedupEventTableConfiguration extends DataFlowStepConfiguration {
    public DedupEventTableConfiguration() {
        setPurgeSources(true);
    }
}
