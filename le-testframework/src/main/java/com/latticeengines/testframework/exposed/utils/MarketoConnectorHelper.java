package com.latticeengines.testframework.exposed.utils;

import com.latticeengines.domain.exposed.pls.MarketoCredential;

public class MarketoConnectorHelper {

    public static final String NAME = "TEST MARKETO CREDENTIAL";
    public static final String SOAP_ENDPOINT = "https://948-IYP-205.mktoapi.com/soap/mktows/2_9";
    public static final String SOAP_USER_ID = "latticeengines1_511435204E14C09D06A6E8";
    public static final String SOAP_ENCRYPTION_KEY = "140990042468919944EE1144CC0099EF0066CF0EE494";
    public static final String REST_ENDPOINT = "https://948-IYP-205.mktorest.com/rest";
    public static final String REST_IDENTITY_ENDPOINT = "https://948-IYP-205.mktorest.com/identity";
    public static final String REST_CLIENT_ID = "dafede33-f785-48d1-85fa-b6ebdb884d06";
    public static final String REST_CLIENT_SECRET = "1R0LCTlmNd7G2PGh9ZJj8SIKSjEVZ8Ik";
    
    public static MarketoCredential getTestMarketoCredentialConfig() {
        MarketoCredential marketoCredential = new MarketoCredential();
        marketoCredential.setName(NAME);
        marketoCredential.setSoapEndpoint(SOAP_ENDPOINT);
        marketoCredential.setSoapUserId(SOAP_USER_ID);
        marketoCredential.setSoapEncryptionKey(SOAP_ENCRYPTION_KEY);
        marketoCredential.setRestEndpoint(REST_ENDPOINT);
        marketoCredential.setRestIdentityEnpoint(REST_IDENTITY_ENDPOINT);
        marketoCredential.setRestClientId(REST_CLIENT_ID);
        marketoCredential.setRestClientSecret(REST_CLIENT_SECRET);
        return marketoCredential;
    }
}
