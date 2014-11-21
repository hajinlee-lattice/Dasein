package com.latticeengines.eai.routes.marketo;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;

public class GenerateAccessTokenUrlProcessor implements Processor {

    @Override
    public void process(Exchange exchange) throws Exception {
        String baseUrl = exchange.getIn().getHeader(MarketoImportProperty.BASEURL, String.class);
        String clientId = exchange.getIn().getHeader(MarketoImportProperty.CLIENTID, String.class);
        String clientSecret = exchange.getIn().getHeader(MarketoImportProperty.CLIENTSECRET, String.class);
        exchange.getIn().setHeader("tokenUrl", new MarketoUrlGenerator().getTokenUrl(baseUrl, clientId, clientSecret));
    }

}
