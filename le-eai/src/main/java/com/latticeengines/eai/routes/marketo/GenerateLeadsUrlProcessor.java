package com.latticeengines.eai.routes.marketo;

import java.util.List;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;

public class GenerateLeadsUrlProcessor implements Processor {

    @SuppressWarnings("unchecked")
    @Override
    public void process(Exchange exchange) throws Exception {
        String baseUrl = exchange.getProperty(MarketoImportProperty.BASEURL, String.class);
        String accessToken = exchange.getProperty(MarketoImportProperty.ACCESSTOKEN, String.class);
        String nextPageToken = exchange.getProperty(MarketoImportProperty.NEXTPAGETOKEN, String.class);
        String filterType = exchange.getProperty(MarketoImportProperty.FILTERTYPE, String.class);
        List<String> filterValues = exchange.getProperty(MarketoImportProperty.FILTERVALUES, List.class);
        exchange.setProperty("leadsUrl",
                new MarketoUrlGenerator().getLeadsUrl(baseUrl, accessToken, nextPageToken, filterType, filterValues));
    }
}
