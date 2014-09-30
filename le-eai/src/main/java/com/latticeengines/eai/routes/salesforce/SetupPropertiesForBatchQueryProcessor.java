package com.latticeengines.eai.routes.salesforce;

import org.apache.camel.Exchange;

import com.latticeengines.eai.routes.PropertySetter;

public class SetupPropertiesForBatchQueryProcessor extends PropertySetter {

    @Override
    public void process(Exchange exchange) throws Exception {
        super.process(exchange);
        exchange.setProperty(SalesforceImportProperty.JOBINFO, exchange.getIn().getBody());
    }

}
