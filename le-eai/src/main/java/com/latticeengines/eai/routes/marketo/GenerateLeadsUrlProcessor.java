package com.latticeengines.eai.routes.marketo;

import java.util.ArrayList;
import java.util.List;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;

public class GenerateLeadsUrlProcessor implements Processor {

    @Override
    public void process(Exchange exchange) throws Exception {
        String baseUrl = exchange.getProperty(MarketoImportProperty.BASEURL, String.class);
        String accessToken = exchange.getProperty(MarketoImportProperty.ACCESSTOKEN, String.class);
        String nextPageToken = exchange.getProperty(MarketoImportProperty.NEXTPAGETOKEN, String.class);
        String filterType = exchange.getProperty(MarketoImportProperty.FILTERTYPE, String.class);
        List<?> filterValuesUnchecked = exchange.getProperty(MarketoImportProperty.FILTERVALUES, List.class);
        List<String> filterValues = new ArrayList<>();
        for (Object object: filterValuesUnchecked) { filterValues.add((String) object); }
        exchange.setProperty("leadsUrl",
                new MarketoUrlGenerator().getLeadsUrl(baseUrl, accessToken, nextPageToken, filterType, filterValues));
    }
}
