package com.latticeengines.eai.routes.marketo;

import java.util.List;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;

public class GenerateLeadsUrlProcessor implements Processor {

    @SuppressWarnings("unchecked")
    @Override
    public void process(Exchange exchange) throws Exception {
        String baseUrl = exchange.getIn().getHeader(MarketoImportProperty.BASEURL, String.class);
        String accessToken = exchange.getIn().getHeader(MarketoImportProperty.ACCESSTOKEN, String.class);
        String nextPageToken = exchange.getIn().getHeader(MarketoImportProperty.NEXTPAGETOKEN, String.class);
        String filterType = exchange.getIn().getHeader(MarketoImportProperty.FILTERTYPE, String.class);
        List<String> filterValues = exchange.getIn().getHeader(MarketoImportProperty.FILTERVALUES, List.class);
        exchange.getIn().setHeader("leadsUrl",
                new MarketoUrlGenerator().getLeadsUrl(baseUrl, accessToken, nextPageToken, filterType, filterValues));
    }
}
