package com.latticeengines.modeling.workflow.steps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dataflow.flows.leadprioritization.DedupType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.modeling.dataflow.DedupEventTableParameters;
import com.latticeengines.domain.exposed.serviceflows.modeling.steps.DedupEventTableConfiguration;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("dedupEventTableDataFlow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
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
                eventTable.getName(), configuration.getDedupType(), configuration.getEventColumn()));
    }

    @Override
    public void onExecutionCompleted() {
        Table eventTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(),
                configuration.getTargetTableName());
        putObjectInContext(EVENT_TABLE, eventTable);
    }
}
