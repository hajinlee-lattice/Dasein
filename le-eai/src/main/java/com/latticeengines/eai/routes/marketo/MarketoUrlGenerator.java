package com.latticeengines.eai.routes.marketo;

import java.util.List;

import org.apache.commons.lang.StringUtils;

public class MarketoUrlGenerator {

    private static final String BASEURL = "https4://$$HOST$$";
    private static final String GETACCESSTOKENURL = "/identity/oauth/token?grant_type=client_credentials&client_id=$$CLIENTID$$&client_secret=$$CLIENTSECRET$$";
    private static final String GETACTIVITIESURL = "/rest/v1/activities.json?access_token=$$ACCESSTOKEN$$&nextPageToken=$$NEXTPAGETOKEN$$&$$ACTIVITYTYPES$$";
    private static final String GETACTIVITYTYPESURL = "/rest/v1/activities/types.json?access_token=$$ACCESSTOKEN$$";
    private static final String GETLEADMETADATAURL = "/rest/v1/leads/describe.json?access_token=$$ACCESSTOKEN$$";
    private static final String GETPAGINGTOKENURL = "/rest/v1/activities/pagingtoken.json?access_token=$$ACCESSTOKEN$$&sinceDatetime=$$SINCEDATETIME$$";
    private static final String GETLEADSURL = "/rest/v1/leads.json?access_token=$$ACCESSTOKEN$$$$NEXTPAGETOKEN$$&filterType=$$FILTERTYPE$$&filterValues$$FILTERVALUES$$";

    public String getBaseUrl(String host) {
        return BASEURL.replace("$$HOST$$", host);
    }

    public String getTokenUrl(String baseUrl, String clientId, String clientSecret) {
        String tokenUrl = GETACCESSTOKENURL.replace("$$CLIENTID$$", clientId);
        tokenUrl = tokenUrl.replace("$$CLIENTSECRET$$", clientSecret);
        return baseUrl + tokenUrl;
    }

    public String getActivitiesUrl(String baseUrl, String accessToken, String nextPageToken, List<String> activityTypes) {
        String activitiesUrl = GETACTIVITIESURL.replace("$$ACCESSTOKEN$$", accessToken);
        // All invocations of the Marketo get activites API requires that a next
        // page token is always available
        activitiesUrl = activitiesUrl.replace("$$NEXTPAGETOKEN$$", nextPageToken);
        activitiesUrl = activitiesUrl.replace("$$ACTIVITYTYPES$$", StringUtils.join(activityTypes, "&"));
        return baseUrl + activitiesUrl;
    }

    public String getActivityTypesUrl(String baseUrl, String accessToken) {
        return baseUrl + GETACTIVITYTYPESURL.replace("$$ACCESSTOKEN$$", accessToken);
    }

    public String getLeadMetadataUrl(String baseUrl, String accessToken) {
        return baseUrl + GETLEADMETADATAURL.replace("$$ACCESSTOKEN$$", accessToken);
    }

    public String getPagingTokenUrl(String baseUrl, String accessToken, String dateTime) {
        String pagingTokenUrl = baseUrl + GETPAGINGTOKENURL.replace("$$ACCESSTOKEN$$", accessToken);
        return pagingTokenUrl.replace("$$SINCEDATETIME$$", dateTime);
    }

    public String getLeadsUrl(String baseUrl, String accessToken, String nextPageToken, String filterType,
            List<String> filterValues) {
        String leadsUrl = GETLEADSURL.replace("$$ACCESSTOKEN$$", accessToken);

        // The first invocation of the Marketo get multiple leads API cannot
        // have a next page token.
        // Only subsequent invocations should have it if the number of returned
        // leads is greater
        // than the batch size.
        if (nextPageToken != null) {
            leadsUrl = leadsUrl.replace("$$NEXTPAGETOKEN$$", nextPageToken);
        }

        leadsUrl = leadsUrl.replace("$$FILTERTYPES$$", filterType);
        leadsUrl = leadsUrl.replace("$$FILTERVALUES$$", StringUtils.join(filterValues, ","));
        return baseUrl + leadsUrl;
    }

}
