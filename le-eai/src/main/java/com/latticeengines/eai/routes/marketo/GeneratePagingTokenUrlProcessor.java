package com.latticeengines.eai.routes.marketo;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;

public class GeneratePagingTokenUrlProcessor implements Processor {

    @Override
    public void process(Exchange exchange) throws Exception {
        String baseUrl = exchange.getProperty(MarketoImportProperty.BASEURL, String.class);
        String accessToken = exchange.getProperty(MarketoImportProperty.ACCESSTOKEN, String.class);
        String sinceDateTime = exchange.getProperty(MarketoImportProperty.SINCEDATETIME, String.class);
        exchange.setProperty("pagingTokenUrl",
                new MarketoUrlGenerator().getPagingTokenUrl(baseUrl, accessToken, sinceDateTime));
    }
}
