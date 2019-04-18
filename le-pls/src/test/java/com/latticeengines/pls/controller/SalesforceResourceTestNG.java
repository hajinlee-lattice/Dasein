package com.latticeengines.pls.controller;

import javax.inject.Inject;

import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.protocol.HttpContext;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.util.SSLUtils;
import com.latticeengines.domain.exposed.pls.SalesforceURL;
import com.latticeengines.pls.entitymanager.SalesforceURLEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBaseDeprecated;
import com.latticeengines.pls.service.SalesforceURLConstants;
import com.latticeengines.pls.service.SalesforceURLService;

public class SalesforceResourceTestNG extends PlsFunctionalTestNGBaseDeprecated {
    private SalesforceURL sfdcURLLPCreated;
    private SalesforceURL sfdcURLAPCreated;

    @Inject
    private SalesforceURLService salesforceURLService;

    @Inject
    private SalesforceURLEntityMgr salesforceURLEntityMgr;

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        // If there is no BisLP/BisAP in SALESFORCE_URL table, insert them
        SalesforceURL sfdcURLLP = salesforceURLEntityMgr.findByURLName(SalesforceURLConstants.BISLP_NAME);
        if (sfdcURLLP == null) {
            sfdcURLLPCreated = createSalesforceURL(SalesforceURLConstants.BISLP_NAME, BISLP_URL);
        }

        SalesforceURL sfdcURLAP = salesforceURLEntityMgr.findByURLName(SalesforceURLConstants.BISAP_NAME);
        if (sfdcURLAP == null) {
            sfdcURLAPCreated = createSalesforceURL(SalesforceURLConstants.BISAP_NAME, BISAP_URL);
        }
    }

    @AfterClass(groups = { "functional" })
    public void tearDown() {
        // Delete records that inserted in setup()
        if (sfdcURLLPCreated != null) {
            salesforceURLEntityMgr.delete(sfdcURLLPCreated);
        }

        if (sfdcURLAPCreated != null) {
            salesforceURLEntityMgr.delete(sfdcURLAPCreated);
        }
    }

    @Test(groups = { "functional" })
    public void bisLP() throws Exception {
        String url = getRestAPIHostPort() + "/pls/salesforce/bis-lp";
        String redirectionURL = getRedirectionURL(url);

        SalesforceURL sfdcURL = salesforceURLEntityMgr.findByURLName(SalesforceURLConstants.BISLP_NAME);
        String sfdcURLLP = sfdcURL.getURL();

        Assert.assertEquals(redirectionURL, sfdcURLLP);
    }

    @Test(groups = { "functional" })
    public void bisLPSandbox() throws Exception {
        String url = getRestAPIHostPort() + "/pls/salesforce/bis-lp-sandbox";
        String redirectionURL = getRedirectionURL(url);

        SalesforceURL sfdcURL = salesforceURLEntityMgr.findByURLName(SalesforceURLConstants.BISLP_NAME);
        String sfdcURLLP = sfdcURL.getURL();
        String sfdcURLLPSandbox = salesforceURLService.getSandboxURL(sfdcURLLP);

        Assert.assertEquals(redirectionURL, sfdcURLLPSandbox);
    }

    @Test(groups = { "functional" })
    public void bisAP() throws Exception {
        String url = getRestAPIHostPort() + "/pls/salesforce/bis-ap";
        String redirectionURL = getRedirectionURL(url);

        SalesforceURL sfdcURL = salesforceURLEntityMgr.findByURLName(SalesforceURLConstants.BISAP_NAME);
        String sfdcURLAP = sfdcURL.getURL();

        Assert.assertEquals(redirectionURL, sfdcURLAP);
    }

    @Test(groups = { "functional" })
    public void bisAPSandbox() throws Exception {
        String url = getRestAPIHostPort() + "/pls/salesforce/bis-ap-sandbox";
        String redirectionURL = getRedirectionURL(url);

        SalesforceURL sfdcURL = salesforceURLEntityMgr.findByURLName(SalesforceURLConstants.BISAP_NAME);
        String sfdcURLAP = sfdcURL.getURL();
        String sfdcURLAPSandbox = salesforceURLService.getSandboxURL(sfdcURLAP);

        Assert.assertEquals(redirectionURL, sfdcURLAPSandbox);
    }

    private String getRedirectionURL(String url) {
        String redirectionURL;
        HttpComponentsClientHttpRequestFactory reqFac = new HttpComponentsClientHttpRequestFactory( //
                HttpClientBuilder.create() //
                        .setConnectionManager(HttpClientUtils.SSL_BLIND_CONNECTION_MGR) //
                        .setRedirectStrategy(new NoRedirectStrategy()) //
                        .build());
        RestTemplate restTemplate = new RestTemplate(reqFac);
        HttpHeaders requestHeaders = new HttpHeaders();
        HttpEntity<String> requestEntity = new HttpEntity<>("", requestHeaders);
        SSLUtils.turnOffSSLNameVerification();
        ResponseEntity<String> responseEntity = restTemplate.exchange(url, HttpMethod.GET, requestEntity, String.class);
        System.out.println(String.format("%s: %d", url, responseEntity.getStatusCode().value()));
        Assert.assertEquals(HttpStatus.FOUND, responseEntity.getStatusCode());
        HttpHeaders responseHeaders = responseEntity.getHeaders();
        redirectionURL = responseHeaders.getLocation().toString();
        System.out.println("got redirection url: " + redirectionURL);
        return redirectionURL;
    }

    private SalesforceURL createSalesforceURL(String name, String url) {
        SalesforceURL sfdcURL = new SalesforceURL();
        sfdcURL.setName(name);
        sfdcURL.setURL(url);
        salesforceURLEntityMgr.create(sfdcURL);
        return sfdcURL;
    }

    private class NoRedirectStrategy extends DefaultRedirectStrategy {
        @Override
        public boolean isRedirected(HttpRequest httpRequest, HttpResponse httpResponse, HttpContext httpContext)  {
            return false;
        }

    }
}
