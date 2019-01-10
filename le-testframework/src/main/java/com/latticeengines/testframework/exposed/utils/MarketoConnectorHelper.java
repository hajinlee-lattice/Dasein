package com.latticeengines.testframework.exposed.utils;

import com.latticeengines.domain.exposed.pls.MarketoCredential;

public class MarketoConnectorHelper {

    /*
     * Using the Configuration from Marketo Soaktest Environment
     */
    public static final String NAME = "TEST MARKETO CREDENTIAL";
    public static final String SOAP_ENDPOINT = "https://549-RCJ-794.mktoapi.com/soap/mktows/3_1";
    public static final String SOAP_USER_ID = "MKTOWS_549-RCJ-794_1";
    public static final String SOAP_ENCRYPTION_KEY = "0932658581430355557755FF44AAFF12CCFFDE762507";
    public static final String REST_ENDPOINT = "https://549-RCJ-794.mktorest.com/rest";
    public static final String REST_IDENTITY_ENDPOINT = "https://549-RCJ-794.mktorest.com/identity";
    public static final String REST_CLIENT_ID = "947f4a35-b424-4aac-bdba-97a41c02e719";
    public static final String REST_CLIENT_SECRET = "k7fG0ESKzRRurxUlmLU6NzSNHNdoixJk";
    
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
