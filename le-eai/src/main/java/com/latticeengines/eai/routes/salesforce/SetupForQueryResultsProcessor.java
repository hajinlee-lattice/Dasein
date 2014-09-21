package com.latticeengines.eai.routes.salesforce;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.component.salesforce.SalesforceEndpointConfig;
import org.apache.camel.component.salesforce.api.dto.bulk.BatchInfo;

public class SetupForQueryResultsProcessor implements Processor {

    @Override
    public void process(Exchange exchange) throws Exception {
        BatchInfo batchInfo = (BatchInfo) exchange.getProperty(SalesforceImportProperty.BATCHINFO);
        exchange.getOut().setBody(batchInfo);
        exchange.getOut().setHeader(SalesforceEndpointConfig.RESULT_ID, exchange.getIn().getBody(String.class));
    }

}
