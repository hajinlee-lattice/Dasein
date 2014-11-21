package com.latticeengines.eai.routes.marketo;

import java.util.List;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;

public class GenerateLeadActivitiesUrlProcessor implements Processor {

    @SuppressWarnings("unchecked")
    @Override
    public void process(Exchange exchange) throws Exception {
        String baseUrl = exchange.getIn().getHeader(MarketoImportProperty.BASEURL, String.class);
        String accessToken = exchange.getIn().getHeader(MarketoImportProperty.ACCESSTOKEN, String.class);
        String nextPageToken = exchange.getIn().getHeader(MarketoImportProperty.NEXTPAGETOKEN, String.class);
        List<Integer> activityTypes = exchange.getIn().getHeader(MarketoImportProperty.ACTIVITYTYPES, List.class);
        exchange.getIn().setHeader("activitiesUrl",
                new MarketoUrlGenerator().getActivitiesUrl(baseUrl, accessToken, nextPageToken, activityTypes));
    }
}
