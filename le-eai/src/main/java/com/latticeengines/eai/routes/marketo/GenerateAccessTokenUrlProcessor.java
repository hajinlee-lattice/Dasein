package com.latticeengines.eai.routes.marketo;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;

public class GenerateAccessTokenUrlProcessor implements Processor {

    @Override
    public void process(Exchange exchange) throws Exception {
        String baseUrl = exchange.getProperty(MarketoImportProperty.BASEURL, String.class);
        String clientId = exchange.getProperty(MarketoImportProperty.CLIENTID, String.class);
        String clientSecret = exchange.getProperty(MarketoImportProperty.CLIENTSECRET, String.class);
        exchange.getIn().setHeader("tokenUrl", new MarketoUrlGenerator().getTokenUrl(baseUrl, clientId, clientSecret));
    }

}
