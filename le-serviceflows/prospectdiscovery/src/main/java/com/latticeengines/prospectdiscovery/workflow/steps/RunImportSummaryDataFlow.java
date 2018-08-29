package com.latticeengines.prospectdiscovery.workflow.steps;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.prospectdiscovery.steps.RunImportSummaryDataFlowConfiguration;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("runImportSummaryDataFlow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RunImportSummaryDataFlow extends RunDataFlow<RunImportSummaryDataFlowConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(RunImportSummaryDataFlow.class);

    @Value("${common.microservice.url}")
    private String microServiceHostPort;

    private Table getEventTable() {
        String eventTableJson = getStringValueFromContext(EVENT_TABLE);
        Table eventTable = JsonUtils.deserialize(eventTableJson, Table.class);
        return eventTable;
    }

    @Override
    public void execute() {
        log.info("Inside RunImportSummaryDataFlow execute()");

        Table eventTable = getEventTable();

        String url = String.format("%s/metadata/customerspaces/%s/tables/%s", microServiceHostPort,
                configuration.getCustomerSpace(), "EventTable");
        restTemplate.delete(url);

        Map<String, String> extraSources = new HashMap<>();
        extraSources.put("EventTable", eventTable.getExtractsDirectory());
        configuration.setExtraSources(extraSources);
        super.execute();
    }

}
