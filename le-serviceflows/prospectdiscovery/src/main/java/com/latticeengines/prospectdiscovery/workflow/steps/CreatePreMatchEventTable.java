package com.latticeengines.prospectdiscovery.workflow.steps;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.prospectdiscovery.steps.CreatePreMatchEventTableConfiguration;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("createPreMatchEventTable")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CreatePreMatchEventTable extends RunDataFlow<CreatePreMatchEventTableConfiguration> {
}
