package com.latticeengines.pls.service;

public interface SalesforceURLService {

    String getBisLP();
    String getBisLPSandbox();
    String getBisAP();
    String getBisAPSandbox();

    String getSandboxURL(String url);
}
