package com.latticeengines.leadprioritization.workflow.steps;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dataflow.flows.leadprioritization.DedupType;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("dedupEventTable")
public class DedupEventTable extends RunDataFlow<DedupEventTableConfiguration> {

    private static final Logger log = Logger.getLogger(DedupEventTable.class);

    @Override
    public void skipStep() {
        log.info(String.format("Not performing dedup because deduplication type is %s",
                DedupType.MULTIPLELEADSPERDOMAIN));
    }
}
