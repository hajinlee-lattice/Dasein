package com.latticeengines.eai.routes.marketo;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;

public class GenerateLeadMetadataUrlProcessor implements Processor {

    @Override
    public void process(Exchange exchange) throws Exception {
        String baseUrl = exchange.getProperty(MarketoImportProperty.BASEURL, String.class);
        String accessToken = exchange.getProperty(MarketoImportProperty.ACCESSTOKEN, String.class);
        exchange.setProperty("leadMetadataUrl", new MarketoUrlGenerator().getLeadMetadataUrl(baseUrl, accessToken));
    }
}
