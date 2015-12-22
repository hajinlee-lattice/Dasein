package com.latticeengines.eai.routes.marketo;

import java.util.ArrayList;
import java.util.List;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;

public class GenerateLeadActivitiesUrlProcessor implements Processor {

    @Override
    public void process(Exchange exchange) throws Exception {
        String baseUrl = exchange.getProperty(MarketoImportProperty.BASEURL, String.class);
        String accessToken = exchange.getProperty(MarketoImportProperty.ACCESSTOKEN, String.class);
        String nextPageToken = exchange.getProperty(MarketoImportProperty.NEXTPAGETOKEN, String.class);

        List<?> activityTypesUnchecked = exchange.getProperty(MarketoImportProperty.ACTIVITYTYPES, List.class);
        List<String> activityTypes = new ArrayList<>();
        for (Object activityTypeUnchecked : activityTypesUnchecked) {
            activityTypes.add((String) activityTypeUnchecked);
        }

        String url = new MarketoUrlGenerator().getActivitiesUrl(baseUrl, accessToken, nextPageToken, activityTypes);
        exchange.setProperty("activitiesUrl", url);
        exchange.setProperty(MarketoImportProperty.ACTIVITYRESULTLIST, new ArrayList<>());
    }
}
