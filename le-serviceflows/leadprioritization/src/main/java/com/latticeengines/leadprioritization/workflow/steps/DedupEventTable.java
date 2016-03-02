package com.latticeengines.leadprioritization.workflow.steps;

import org.springframework.stereotype.Component;

import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("dedupEventTable")
public class DedupEventTable extends RunDataFlow<DedupEventTableConfiguration> {
}
