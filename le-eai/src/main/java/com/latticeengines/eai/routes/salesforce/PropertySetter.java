package com.latticeengines.eai.routes.salesforce;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;

public class PropertySetter implements Processor {

    @Override
    public void process(Exchange exchange) throws Exception {
        exchange.setProperty(SalesforceImportHeader.TABLE, exchange.getIn().getHeader(SalesforceImportHeader.TABLE));
        exchange.setProperty(SalesforceImportHeader.JOBINFO, exchange.getIn().getBody());
    }

}
