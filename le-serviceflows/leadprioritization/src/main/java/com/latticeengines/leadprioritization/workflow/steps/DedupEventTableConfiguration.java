package com.latticeengines.leadprioritization.workflow.steps;

import com.latticeengines.domain.exposed.dataflow.flows.leadprioritization.DedupType;
import com.latticeengines.serviceflows.workflow.dataflow.DataFlowStepConfiguration;

public class DedupEventTableConfiguration extends DataFlowStepConfiguration {
    
    private DedupType deduplicationType = DedupType.ONELEADPERDOMAIN;
    
    public DedupEventTableConfiguration() {
    }
    
    public void setDeduplicationType(DedupType deduplicationType) {
        this.deduplicationType = deduplicationType;
    }
    
    public DedupType getDeduplicationType() {
        return deduplicationType;
    }
    
}
