package com.latticeengines.prospectdiscovery.workflow.steps;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("runImportSummaryDataFlow")
public class RunImportSummaryDataFlow extends RunDataFlow<RunImportSummaryDataFlowConfiguration> {

    private static final Log log = LogFactory.getLog(RunImportSummaryDataFlow.class);

    private Table getEventTable() {
        String eventTableJson = getStringValueFromContext(EVENT_TABLE);
        Table eventTable = JsonUtils.deserialize(eventTableJson, Table.class);
        return eventTable;
    }

    @Override
    public void execute() {
        log.info("Inside RunImportSummaryDataFlow execute()");

        Table eventTable = getEventTable();

        String url = String.format("%s/metadata/customerspaces/%s/tables/%s", configuration.getMicroServiceHostPort(),
                configuration.getCustomerSpace(), "EventTable");
        restTemplate.delete(url);

        Map<String, String> extraSources = new HashMap<>();
        extraSources.put("EventTable", eventTable.getExtractsDirectory());
        configuration.setExtraSources(extraSources);
        super.execute();
    }

}
