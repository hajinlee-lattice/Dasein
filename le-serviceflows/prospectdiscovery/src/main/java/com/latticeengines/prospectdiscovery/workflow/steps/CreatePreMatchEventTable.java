package com.latticeengines.prospectdiscovery.workflow.steps;

import com.latticeengines.domain.exposed.serviceflows.prospectdiscovery.steps.CreatePreMatchEventTableConfiguration;
import org.springframework.stereotype.Component;

import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("createPreMatchEventTable")
public class CreatePreMatchEventTable extends RunDataFlow<CreatePreMatchEventTableConfiguration> {
}
