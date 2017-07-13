package com.latticeengines.leadprioritization.workflow.steps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dataflow.flows.leadprioritization.DedupType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.dataflow.DedupEventTableParameters;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.DedupEventTableConfiguration;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("dedupEventTableDataFlow")
public class DedupEventTable extends RunDataFlow<DedupEventTableConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(DedupEventTable.class);

    @Override
    public void skipStep() {
        log.info(String.format("Not performing dedup because deduplication type is %s",
                DedupType.MULTIPLELEADSPERDOMAIN));
    }

    @Override
    public void onConfigurationInitialized() {
        DedupEventTableConfiguration configuration = getConfiguration();
        Table eventTable = getObjectFromContext(EVENT_TABLE, Table.class);
        configuration.setTargetTableName(eventTable.getName() + "_deduped");
        configuration.setDataFlowParams(new DedupEventTableParameters( //
                eventTable.getName(), configuration.getDedupType()));
    }

    @Override
    public void onExecutionCompleted() {
        Table eventTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(),
                configuration.getTargetTableName());
        putObjectInContext(EVENT_TABLE, eventTable);
    }
}
