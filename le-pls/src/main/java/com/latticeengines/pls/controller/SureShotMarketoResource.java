package com.latticeengines.pls.controller;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.pls.MarketoCredential;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "sureshotmarketo", description = "REST resource for providing SureShot with Marketo Credentials")
@RestController
@RequestMapping(value = "/sureshot/marketo")
@PreAuthorize("hasRole('View_PLS_Sureshot')")
public class SureShotMarketoResource {

    private static final Logger log = Logger.getLogger(SureShotResource.class);

    private static final MarketoCredential MARKETO_CREDENTIAL = new MarketoCredential();

    private static final Long ID = 1L;
    private static final String NAME = "TEST MARKETO CREDENTIAL";
    private static final String SOAP_ENDPOINT = "https://948-IYP-205.mktoapi.com/soap/mktows/2_9";
    private static final String SOAP_USER_ID = "latticeengines1_511435204E14C09D06A6E8";
    private static final String SOAP_ENCRYPTION_KEY = "140990042468919944EE1144CC0099EF0066CF0EE494";
    private static final String REST_ENDPOINT = "https://948-IYP-205.mktorest.com/rest";
    private static final String REST_IDENTITY_ENDPOINT = "https://948-IYP-205.mktorest.com/identity";
    private static final String REST_CLIENT_ID = "dafede33-f785-48d1-85fa-b6ebdb884d06";
    private static final String REST_CLIENT_SECRET = "1R0LCTlmNd7G2PGh9ZJj8SIKSjEVZ8Ik";

    static {
        MARKETO_CREDENTIAL.setPid(ID);
        MARKETO_CREDENTIAL.setName(NAME);
        MARKETO_CREDENTIAL.setSoapEndpoint(SOAP_ENDPOINT);
        MARKETO_CREDENTIAL.setSoapUserId(SOAP_USER_ID);
        MARKETO_CREDENTIAL.setSoapEncrytionKey(SOAP_ENCRYPTION_KEY);
        MARKETO_CREDENTIAL.setRestEndpoint(REST_ENDPOINT);
        MARKETO_CREDENTIAL.setRestIdentityEnpoint(REST_IDENTITY_ENDPOINT);
        MARKETO_CREDENTIAL.setRestClientId(REST_CLIENT_ID);
        MARKETO_CREDENTIAL.setRestClientSecret(REST_CLIENT_SECRET);
    }

    @RequestMapping(value = "/{credentialId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ApiOperation(value = "Get marketo credential by id")
    @ResponseBody
    public MarketoCredential find(@PathVariable String credentialId) {
        return MARKETO_CREDENTIAL;
    }

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ApiOperation(value = "Get all marketo credentials")
    @ResponseBody
    public List<MarketoCredential> findAll() {
        return Arrays.asList(MARKETO_CREDENTIAL);
    }

}
