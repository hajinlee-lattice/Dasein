package com.latticeengines.eai.routes.marketo;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;

public class GenerateBaseUrlProcessor implements Processor {

    @Override
    public void process(Exchange exchange) throws Exception {
        String host = exchange.getProperty(MarketoImportProperty.HOST, String.class);
        exchange.setProperty("baseUrl", new MarketoUrlGenerator().getBaseUrl(host));
    }

}
