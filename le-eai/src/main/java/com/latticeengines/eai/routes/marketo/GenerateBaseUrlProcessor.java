package com.latticeengines.eai.routes.marketo;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;

public class GenerateBaseUrlProcessor implements Processor {

    @Override
    public void process(Exchange exchange) throws Exception {
        String host = exchange.getIn().getHeader(MarketoImportProperty.HOST, String.class);
        exchange.getIn().setHeader("baseUrl", new MarketoUrlGenerator().getBaseUrl(host));
    }

}
