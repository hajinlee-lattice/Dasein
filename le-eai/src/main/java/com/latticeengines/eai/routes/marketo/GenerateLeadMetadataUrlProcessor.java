package com.latticeengines.eai.routes.marketo;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;

public class GenerateLeadMetadataUrlProcessor implements Processor {

    @Override
    public void process(Exchange exchange) throws Exception {
        String baseUrl = exchange.getIn().getHeader(MarketoImportProperty.BASEURL, String.class);
        String accessToken = exchange.getIn().getHeader(MarketoImportProperty.ACCESSTOKEN, String.class);
        exchange.getIn().setHeader("leadMetadataUrl",
                new MarketoUrlGenerator().getLeadMetadataUrl(baseUrl, accessToken));
    }
}
