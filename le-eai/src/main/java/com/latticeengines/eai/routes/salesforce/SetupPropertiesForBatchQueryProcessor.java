package com.latticeengines.eai.routes.salesforce;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;

public class SetupPropertiesForBatchQueryProcessor implements Processor {

    @Override
    public void process(Exchange exchange) throws Exception {
        exchange.setProperty(SalesforceImportProperty.TABLE, exchange.getIn().getHeader(SalesforceImportProperty.TABLE));
        exchange.setProperty(SalesforceImportProperty.JOBINFO, exchange.getIn().getBody());
    }

}
