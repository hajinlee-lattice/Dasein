package com.latticeengines.leadprioritization.workflow.steps;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dataflow.flows.leadprioritization.DedupType;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("dedupEventTable")
public class DedupEventTable extends RunDataFlow<DedupEventTableConfiguration> {
    
    @Override
    public void execute() {
        if (configuration.getDeduplicationType() == DedupType.ONELEADPERDOMAIN) {
            super.execute();
        }
    }

}
