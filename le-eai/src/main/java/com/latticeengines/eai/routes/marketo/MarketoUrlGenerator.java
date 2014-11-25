package com.latticeengines.eai.routes.marketo;

import java.util.List;

import org.apache.commons.lang.StringUtils;

public class MarketoUrlGenerator {

    private static final String BASEURL = "https4://$$HOST$$";
    private static final String GETACCESSTOKENURL = "/identity/oauth/token?grant_type=client_credentials&client_id=$$CLIENTID$$&client_secret=$$CLIENTSECRET$$";
    private static final String GETACTIVITIESURL = "/rest/v1/activities.json?access_token=$$ACCESSTOKEN$$&$$NEXTPAGETOKEN$$&$$ACTIVITYTYPES$$";
    private static final String GETACTIVITYTYPESURL = "/rest/v1/activities/types.json?access_token=$$ACCESSTOKEN$$";
    private static final String GETLEADMETADATAURL = "/rest/v1/leads/describe.json?access_token=$$ACCESSTOKEN$$";
    private static final String GETPAGINGTOKENURL = "/rest/v1/activities/pagingtoken.json?access_token=$$ACCESSTOKEN$$&sinceDatetime=$$SINCEDATETIME$$";

    public String getBaseUrl(String host) {
        return BASEURL.replace("$$HOST$$", host);
    }

    public String getTokenUrl(String baseUrl, String clientId, String clientSecret) {
        String tokenUrl = GETACCESSTOKENURL.replace("$$CLIENTID$$", clientId);
        tokenUrl = tokenUrl.replace("$$CLIENTSECRET$$", clientSecret);
        return baseUrl + tokenUrl;
    }

    public String getActivitiesUrl(String baseUrl, String accessToken, String nextPageToken, List<Integer> activityTypes) {
        String activitiesUrl = GETACTIVITIESURL.replace("$$ACCESSTOKEN$$", accessToken);
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
}
