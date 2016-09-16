package com.latticeengines.pls.controller;

import java.util.Arrays;
import java.util.List;

import com.latticeengines.pls.service.MarketoCredentialService;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.pls.Enrichment;
import com.latticeengines.domain.exposed.pls.MarketoCredential;
import com.latticeengines.domain.exposed.pls.MarketoMatchField;
import com.latticeengines.domain.exposed.pls.MarketoMatchFieldName;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "marketo", description = "REST resource for marketo credentials")
@RestController
@RequestMapping("/marketo/credentials")
@PreAuthorize("hasRole('View_PLS_MarketoCredential')")
public class MarketoCredentialResource {

    private static final Logger log = Logger.getLogger(ModelSummaryResource.class);

    @Value("${pls.marketo.enrichment.webhook.url}")
    private static String enrichmentWebhookUrl;

    // @Autowired
    // private MarketoCredentialService marketoCredentialService;

    private static final MarketoCredential MARKETO_CREDENTIAL = new MarketoCredential();
    private static final Enrichment ENRICHMENT = new Enrichment();

    private static final String NAME = "TEST MARKETO CREDENTIAL";
    private static final String SOAP_ENDPOINT = "https://948-IYP-205.mktoapi.com/soap/mktows/2_9";
    private static final String SOAP_USER_ID = "latticeengines1_511435204E14C09D06A6E8";
    private static final String SOAP_ENCRYPTION_KEY = "140990042468919944EE1144CC0099EF0066CF0EE494";
    private static final String REST_ENDPOINT = "https://948-IYP-205.mktorest.com/rest";
    private static final String REST_IDENTITY_ENDPOINT = "https://948-IYP-205.mktorest.com/identity";
    private static final String REST_CLIENT_ID = "dafede33-f785-48d1-85fa-b6ebdb884d06";
    private static final String REST_CLIENT_SECRET = "1R0LCTlmNd7G2PGh9ZJj8SIKSjEVZ8Ik";

    private static final MarketoMatchField MARKETO_MATCH_FIELD_1 = new MarketoMatchField();
    private static final MarketoMatchField MARKETO_MATCH_FIELD_2 = new MarketoMatchField();
    private static final MarketoMatchField MARKETO_MATCH_FIELD_3 = new MarketoMatchField();
    private static final MarketoMatchField MARKETO_MATCH_FIELD_4 = new MarketoMatchField();
    private static final String MARKETO_FIELD_NAME_1 = "Lead Email";
    private static final String MARKETO_FIELD_NAME_2 = "Lead Company";
    private static final String MARKETO_FIELD_NAME_3 = "Lead State";
    private static final String MARKETO_FIELD_NAME_4 = "Lead Country";
    private static final String ENRICHMENT_TENENT_CREDENTIAL_GUID = "1111111111-1111111-111111111111-11111";

    static {
        MARKETO_CREDENTIAL.setName(NAME);
        MARKETO_CREDENTIAL.setSoapEndpoint(SOAP_ENDPOINT);
        MARKETO_CREDENTIAL.setSoapUserId(SOAP_USER_ID);
        MARKETO_CREDENTIAL.setSoapEncrytionKey(SOAP_ENCRYPTION_KEY);
        MARKETO_CREDENTIAL.setRestEndpoint(REST_ENDPOINT);
        MARKETO_CREDENTIAL.setRestIdentityEnpoint(REST_IDENTITY_ENDPOINT);
        MARKETO_CREDENTIAL.setRestClientId(REST_CLIENT_ID);
        MARKETO_CREDENTIAL.setRestClientSecret(REST_CLIENT_SECRET);
        ENRICHMENT.setMarketoMatchFields(Arrays.asList(MARKETO_MATCH_FIELD_1, MARKETO_MATCH_FIELD_2,
                MARKETO_MATCH_FIELD_3, MARKETO_MATCH_FIELD_4));
        MARKETO_CREDENTIAL.setEnrichment(ENRICHMENT);

        MARKETO_MATCH_FIELD_1.setMarketoMatchFieldName(MarketoMatchFieldName.Domain);
        MARKETO_MATCH_FIELD_1.setMarketoFieldName(MARKETO_FIELD_NAME_1);
        MARKETO_MATCH_FIELD_1.setEnrichment(ENRICHMENT);
        MARKETO_MATCH_FIELD_2.setMarketoMatchFieldName(MarketoMatchFieldName.Company);
        MARKETO_MATCH_FIELD_2.setMarketoFieldName(MARKETO_FIELD_NAME_2);
        MARKETO_MATCH_FIELD_2.setEnrichment(ENRICHMENT);
        MARKETO_MATCH_FIELD_3.setMarketoMatchFieldName(MarketoMatchFieldName.State);
        MARKETO_MATCH_FIELD_3.setMarketoFieldName(MARKETO_FIELD_NAME_3);
        MARKETO_MATCH_FIELD_3.setEnrichment(ENRICHMENT);
        MARKETO_MATCH_FIELD_4.setMarketoMatchFieldName(MarketoMatchFieldName.Country);
        MARKETO_MATCH_FIELD_4.setMarketoFieldName(MARKETO_FIELD_NAME_4);
        MARKETO_MATCH_FIELD_4.setEnrichment(ENRICHMENT);
        ENRICHMENT.setTenantCredentialGUID(ENRICHMENT_TENENT_CREDENTIAL_GUID);
        ENRICHMENT.setWebhookUrl(enrichmentWebhookUrl);
    }

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a marketo credential")
    @PreAuthorize("hasRole('Edit_PLS_MarketoCredential')")
    public void create(@RequestBody MarketoCredential marketoCredential) {
        // marketoCredentialService.createMarketoCredential(marketoCredential);
    }

    @RequestMapping(value = "/{credentialName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ApiOperation(value = "Get marketo credential by name")
    @ResponseBody
    public MarketoCredential find(@PathVariable String credentialName) {
        return MARKETO_CREDENTIAL;
        // return marketoCredentialService.findMarketoCredentialByName(credentialName);
    }

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ApiOperation(value = "Get all marketo credentials")
    @ResponseBody
    public List<MarketoCredential> findAll() {
        return Arrays.asList(MARKETO_CREDENTIAL);
    }

    @RequestMapping(value = "/{credentialName}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ApiOperation(value = "Delete a marketo credential")
    @PreAuthorize("hasRole('Edit_PLS_MarketoCredential')")
    public void delete(@PathVariable String credentialName) {
        // marketoCredentialService.deleteMarketoCredentialByName(credentialName);
    }

    @RequestMapping(value = "/{credentialName}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ApiOperation(value = "Updates a marketo credential")
    @PreAuthorize("hasRole('Edit_PLS_MarketoCredential')")
    public void update(@PathVariable String credentialName,
            @RequestBody MarketoCredential credential) {
        // marketoCredentialService.updateMarketoCredentialByName(credentialName, credential);
    }

    @RequestMapping(value = "/enrichment", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ApiOperation(value = "Updates a enrichment mathcing fields")
    @PreAuthorize("hasRole('Edit_PLS_MarketoCredential')")
    public void updateEnrichment(@RequestBody List<MarketoMatchField> marketoMatchFields) {

    }

}
