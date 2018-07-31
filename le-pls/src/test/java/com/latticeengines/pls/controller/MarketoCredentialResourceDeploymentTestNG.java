package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.MarketoCredential;
import com.latticeengines.domain.exposed.pls.MarketoScoringMatchField;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfig;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfigSummary;
import com.latticeengines.domain.exposed.scoringapi.FieldInterpretation;
import com.latticeengines.pls.entitymanager.MarketoCredentialEntityMgr;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

public class MarketoCredentialResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final String PLS_MARKETO_CREDENTIAL_URL = "/pls/marketo/credentials";
    private static final String PLS_MARKETO_SCORING_REQUESTS = "/scoring-requests";

    @Autowired
    private MarketoCredentialEntityMgr marketoCredentialEntityMgr;
    
    @Value("${pls.marketo.enrichment.webhook.url}")
    private String enrichmentWebhookUrl;
    
    @Value("${pls.marketo.scoring.webhook.resource}")
    private String scoringWebhookResource;
    
    private static final String CREDENTIAL_NAME = "TEST-DP-MARKETO-SCORING-CONFIG-";
    private static final String SOAP_ENDPOINT = "TEST_SOAP_EP";
    private static final String SOAP_USER_ID = "TEST_SOAP_UID";
    private static final String SOAP_ENCRYPTION_KEY = "TEST_SOAP_ENCRYPTION";
    private static final String REST_ENDPOINT = "TEST_REST_EP";
    private static final String REST_IDENTITY_ENDPOINT = "TEST_REST_IDENTITY_EP";
    private static final String REST_CLIENT_ID = "TEST_REST_CLIENTID";
    private static final String REST_CLIENT_SECRET = "TEST_REST_CLIENTSECRET";
        
    // Scoring Constants
    private static final String MODEL_UUID = "ms__0f4217c2-f234-443a-af42-6d7b7a7ff9f3-PLSModel";
    
    private static final MarketoScoringMatchField MARKETO_SCORE_MATCH_FIELD_1 = new MarketoScoringMatchField();
    private static final MarketoScoringMatchField MARKETO_SCORE_MATCH_FIELD_2 = new MarketoScoringMatchField();
    private static final MarketoScoringMatchField MARKETO_SCORE_MATCH_FIELD_3 = new MarketoScoringMatchField();
    private static final MarketoScoringMatchField MARKETO_SCORE_MATCH_FIELD_4 = new MarketoScoringMatchField();
    private static final MarketoScoringMatchField MARKETO_SCORE_MATCH_FIELD_5 = new MarketoScoringMatchField();
    private static final MarketoScoringMatchField MARKETO_SCORE_MATCH_FIELD_6 = new MarketoScoringMatchField();
    
    // TestClass context for CredentialID
    private Long marketoCredentialId = null;
    
    @BeforeClass(groups = { "deployment" })
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenant();
        System.out.println("********* setup: " + mainTestTenant);
        MultiTenantContext.setTenant(mainTestTenant);
    }

    @Test(groups = { "deployment" })
    public void findMarketoCredentialAsDifferentUsers_assertCorrectBehavior() {
        switchToThirdPartyUser();
        assertGetSimplifiedCredentialsSuccess();
        assertGetFullCredentialsFailed();

        switchToExternalUser();
        assertGetSimplifiedCredentialsSuccess();
        assertGetFullCredentialsSuccess();

        switchToInternalAdmin();
        assertGetSimplifiedCredentialsSuccess();
        assertGetFullCredentialsSuccess();
    }

    @Test(groups = {"deployment"}, dependsOnMethods = "findMarketoCredentialAsDifferentUsers_assertCorrectBehavior")
    public void createMarketoCredential_assertCreation() {
        // Using the EntityManager to create the Credential.
        // If I use REST API to create credential, it will go through SOAP and REST key validations.
        // If we rely on validations, then there is a possibility of tests will fail when the Marketo is down / its keys changes
        MarketoCredential marketoCredential = new MarketoCredential();
        String credName = CREDENTIAL_NAME + System.currentTimeMillis();
        marketoCredential.setName(credName);
        marketoCredential.setSoapEndpoint(SOAP_ENDPOINT);
        marketoCredential.setSoapUserId(SOAP_USER_ID);
        marketoCredential.setSoapEncryptionKey(SOAP_ENCRYPTION_KEY);
        marketoCredential.setRestEndpoint(REST_ENDPOINT);
        marketoCredential.setRestIdentityEnpoint(REST_IDENTITY_ENDPOINT);
        marketoCredential.setRestClientId(REST_CLIENT_ID);
        marketoCredential.setRestClientSecret(REST_CLIENT_SECRET);

        marketoCredentialEntityMgr.create(marketoCredential);
        
        List<MarketoCredential> credentialList = getMarketoCredentials(PLS_MARKETO_CREDENTIAL_URL+ "/simplified");
        marketoCredential = credentialList.get(0);
        assertMarketoCrendetial(marketoCredential, credName, true);
        
        credentialList = getMarketoCredentials(PLS_MARKETO_CREDENTIAL_URL);
        marketoCredential = credentialList.get(0);
        assertMarketoCrendetial(marketoCredential, credName, false);
        
        marketoCredentialId = marketoCredential.getPid();
        MarketoCredential marketoCredentialFromRest = getMarketoCredential(marketoCredentialId);
        assertMarketoCrendetial(marketoCredentialFromRest, credName, false);
    }

    
    @Test(groups = {"deployment"}, dependsOnMethods = "createMarketoCredential_assertCreation")
    public void testCreateScoringRequestConfig_assertCreation() {
        MarketoCredential marketoCredential = getMarketoCredential(marketoCredentialId);
        assertNotNull(marketoCredential);
        List<ScoringRequestConfigSummary> reqConfigSummaryLst = getScoringRequestConfigs(marketoCredential.getPid());
        assertNotNull(reqConfigSummaryLst);
        assertTrue(reqConfigSummaryLst.size() == 0);
        
        ScoringRequestConfig scoringReqConf = new ScoringRequestConfig();
        scoringReqConf.setMarketoCredential(marketoCredential);
        scoringReqConf.setModelUuid(MODEL_UUID);
        
        MARKETO_SCORE_MATCH_FIELD_1.setModelFieldName(FieldInterpretation.Website.toString());
        MARKETO_SCORE_MATCH_FIELD_1.setMarketoFieldName("lead.website");
        
        MARKETO_SCORE_MATCH_FIELD_2.setModelFieldName(FieldInterpretation.Email.toString());
        MARKETO_SCORE_MATCH_FIELD_2.setMarketoFieldName("lead.email");
        
        MARKETO_SCORE_MATCH_FIELD_3.setModelFieldName(FieldInterpretation.CompanyName.toString());
        MARKETO_SCORE_MATCH_FIELD_3.setMarketoFieldName("lead.company");
        List<MarketoScoringMatchField> scoringMappings = Arrays.asList(MARKETO_SCORE_MATCH_FIELD_1, MARKETO_SCORE_MATCH_FIELD_2, MARKETO_SCORE_MATCH_FIELD_3);
        scoringReqConf.setMarketoScoringMatchFields(scoringMappings);
        
        createScoringRequestConfig(marketoCredential.getPid(), scoringReqConf);
        
        reqConfigSummaryLst = getScoringRequestConfigs(marketoCredential.getPid());
        assertNotNull(reqConfigSummaryLst);
        assertTrue(reqConfigSummaryLst.size() == 1);
        
        assertNotNull(reqConfigSummaryLst.get(0).getModelUuid());
        assertNotNull(reqConfigSummaryLst.get(0).getConfigId());
        
        ScoringRequestConfig scoringReqConfFromRest = getScoringRequestConfig(marketoCredential.getPid(), reqConfigSummaryLst.get(0).getConfigId());
        assertNotNull(scoringReqConfFromRest);
        assertNotNull(scoringReqConfFromRest.getMarketoScoringMatchFields());
        assertEquals(scoringReqConfFromRest.getMarketoScoringMatchFields().size(), scoringMappings.size());
        assertEquals(scoringReqConfFromRest.getWebhookResource(), scoringWebhookResource);
    }
    
    @Test(groups = "deployment", dependsOnMethods = "testCreateScoringRequestConfig_assertCreation")
    public void testCreateScoringRequestConfigDuplicate_assertCreationFails() {
        MarketoCredential marketoCredential = getMarketoCredential(marketoCredentialId);
        assertNotNull(marketoCredential);
        List<ScoringRequestConfigSummary> reqConfigSummaryLst = getScoringRequestConfigs(marketoCredential.getPid());
        assertNotNull(reqConfigSummaryLst);
        assertTrue(reqConfigSummaryLst.size() == 1);
        
        ScoringRequestConfig scoringReqConf = new ScoringRequestConfig();
        scoringReqConf.setMarketoCredential(marketoCredential);
        scoringReqConf.setModelUuid(MODEL_UUID);
        
        MARKETO_SCORE_MATCH_FIELD_1.setModelFieldName(FieldInterpretation.Website.toString());
        MARKETO_SCORE_MATCH_FIELD_1.setMarketoFieldName("lead.website");
        
        MARKETO_SCORE_MATCH_FIELD_2.setModelFieldName(FieldInterpretation.Email.toString());
        MARKETO_SCORE_MATCH_FIELD_2.setMarketoFieldName("lead.email");
        
        MARKETO_SCORE_MATCH_FIELD_3.setModelFieldName(FieldInterpretation.CompanyName.toString());
        MARKETO_SCORE_MATCH_FIELD_3.setMarketoFieldName("lead.company");
        List<MarketoScoringMatchField> scoringMappings = Arrays.asList(MARKETO_SCORE_MATCH_FIELD_1, MARKETO_SCORE_MATCH_FIELD_2, MARKETO_SCORE_MATCH_FIELD_3);
        scoringReqConf.setMarketoScoringMatchFields(scoringMappings);
        
        
        Exception exception = null;
        try {
            createScoringRequestConfig(marketoCredential.getPid(), scoringReqConf);
        } catch (Exception e) {
            exception = e;
        }
        
        assertNotNull(exception);
        assertTrue(exception.getMessage().contains(LedpException.buildMessage(LedpCode.LEDP_18192, new String[] {marketoCredential.getName()})));
        
        reqConfigSummaryLst = getScoringRequestConfigs(marketoCredential.getPid());
        assertNotNull(reqConfigSummaryLst);
        assertTrue(reqConfigSummaryLst.size() == 1);
    }
    
    @Test(groups = "deployment", dependsOnMethods = "testCreateScoringRequestConfig_assertCreation")
    public void testFindMethods_assertRetrievals() {
        List<ScoringRequestConfigSummary> requestConfigLst = getScoringRequestConfigs(marketoCredentialId);
        assertNotNull(requestConfigLst);
        assertTrue(requestConfigLst.size() == 1);
        ScoringRequestConfigSummary configSummary = requestConfigLst.get(0);
        assertNotNull(configSummary.getConfigId());
        assertNotNull(configSummary.getModelUuid());
        
        ScoringRequestConfig scoreReqConf2 = getScoringRequestConfig(marketoCredentialId, configSummary.getConfigId());
        assertNotNull(scoreReqConf2);
        assertNotNull(scoreReqConf2.getMarketoScoringMatchFields());
        assertEquals(scoreReqConf2.getWebhookResource(), scoringWebhookResource);
    }
    
    @Test(groups = "deployment", dependsOnMethods = "testFindMethods_assertRetrievals")
    public void testUpdateScoringRequestConfig_assertFieldAdditions() {
        List<ScoringRequestConfigSummary> requestConfigLst = getScoringRequestConfigs(marketoCredentialId);
        ScoringRequestConfigSummary configSummary = requestConfigLst.get(0);
        ScoringRequestConfig scoreReqConf1 = getScoringRequestConfig(marketoCredentialId, configSummary.getConfigId());
        assertNotNull(scoreReqConf1);
        
        MARKETO_SCORE_MATCH_FIELD_4.setModelFieldName(FieldInterpretation.DUNS.toString());
        MARKETO_SCORE_MATCH_FIELD_4.setMarketoFieldName("lead.DUNS");
        
        MARKETO_SCORE_MATCH_FIELD_5.setModelFieldName(FieldInterpretation.Domain.toString());
        MARKETO_SCORE_MATCH_FIELD_5.setMarketoFieldName("lead.Domain");
        
        List<MarketoScoringMatchField> updatedMatchFields = new ArrayList<>(scoreReqConf1.getMarketoScoringMatchFields());
        updatedMatchFields.add(MARKETO_SCORE_MATCH_FIELD_4);
        updatedMatchFields.add(MARKETO_SCORE_MATCH_FIELD_5);
        scoreReqConf1.setMarketoScoringMatchFields(updatedMatchFields);
        
        updateScoringRequestConfig(marketoCredentialId, scoreReqConf1);
        ScoringRequestConfig scoreReqConf2 = getScoringRequestConfig(marketoCredentialId, scoreReqConf1.getConfigId());
        assertNotNull(scoreReqConf2);
        
        assertEquals(scoreReqConf2.getMarketoScoringMatchFields().size(), updatedMatchFields.size());
    }

    @Test(groups = "deployment", dependsOnMethods = "testUpdateScoringRequestConfig_assertFieldAdditions")
    public void testUpdateScoringRequestConfig_assertFieldRemoval() {
        List<ScoringRequestConfigSummary> requestConfigLst = getScoringRequestConfigs(marketoCredentialId);
        ScoringRequestConfigSummary configSummary = requestConfigLst.get(0);
        ScoringRequestConfig scoreReqConf1 = getScoringRequestConfig(marketoCredentialId, configSummary.getConfigId());
        assertNotNull(scoreReqConf1);
        
        List<MarketoScoringMatchField> updatedMatchFields = scoreReqConf1.getMarketoScoringMatchFields().subList(0, 3);
        scoreReqConf1.setMarketoScoringMatchFields(updatedMatchFields);
        
        updateScoringRequestConfig(marketoCredentialId, scoreReqConf1);
        ScoringRequestConfig scoreReqConf2 = getScoringRequestConfig(marketoCredentialId, scoreReqConf1.getConfigId());
        assertNotNull(scoreReqConf2);
        
        assertEquals(scoreReqConf2.getMarketoScoringMatchFields().size(), updatedMatchFields.size());
    }
    
    @Test(groups = "deployment", dependsOnMethods = "testUpdateScoringRequestConfig_assertFieldRemoval")
    public void testUpdateScoringRequestConfig_assertFieldsAddAndUpdate() {
        List<ScoringRequestConfigSummary> requestConfigLst = getScoringRequestConfigs(marketoCredentialId);
        ScoringRequestConfigSummary configSummary = requestConfigLst.get(0);
        ScoringRequestConfig scoreReqConf1 = getScoringRequestConfig(marketoCredentialId, configSummary.getConfigId());
        assertNotNull(scoreReqConf1);
        
        MARKETO_SCORE_MATCH_FIELD_6.setModelFieldName(FieldInterpretation.Industry.toString());
        MARKETO_SCORE_MATCH_FIELD_6.setMarketoFieldName("lead.Industry");
        
        List<MarketoScoringMatchField> updatedMatchFields = new ArrayList<>(scoreReqConf1.getMarketoScoringMatchFields());
        updatedMatchFields.add(MARKETO_SCORE_MATCH_FIELD_6);
        scoreReqConf1.setMarketoScoringMatchFields(updatedMatchFields);
        
        updatedMatchFields.forEach(matchField -> matchField.setMarketoFieldName(matchField.getMarketoFieldName() + "-Updated"));
        
        updateScoringRequestConfig(marketoCredentialId, scoreReqConf1);
        
        ScoringRequestConfig scoreReqConf2 = getScoringRequestConfig(marketoCredentialId, scoreReqConf1.getConfigId());
        assertNotNull(scoreReqConf2);
        
        assertEquals(scoreReqConf2.getMarketoScoringMatchFields().size(), updatedMatchFields.size());
        scoreReqConf2.getMarketoScoringMatchFields().forEach(matchField -> assertTrue(matchField.getMarketoFieldName().endsWith("-Updated")));
    }
    
    
    private void createScoringRequestConfig(Long credentialId, ScoringRequestConfig scoringReqConf) {
        restTemplate.postForObject(getRestAPIHostPort() + PLS_MARKETO_CREDENTIAL_URL + "/" + credentialId + PLS_MARKETO_SCORING_REQUESTS, scoringReqConf, ScoringRequestConfig.class);
    }

    private void updateScoringRequestConfig(Long credentialId, ScoringRequestConfig scoreReqConf) {
        restTemplate.put(getRestAPIHostPort() + PLS_MARKETO_CREDENTIAL_URL + "/" + credentialId + PLS_MARKETO_SCORING_REQUESTS + "/" + scoreReqConf.getConfigId(), scoreReqConf);
    }
    
    private void assertGetSimplifiedCredentialsSuccess() {
        List response = restTemplate.getForObject(
                getRestAPIHostPort() + PLS_MARKETO_CREDENTIAL_URL + "/simplified", List.class);
        assertNotNull(response);
        assertEquals(response.size(), 0);
    }

    private void assertGetFullCredentialsSuccess() {
        List response = restTemplate.getForObject(
                getRestAPIHostPort() + PLS_MARKETO_CREDENTIAL_URL, List.class);
        assertNotNull(response);
        assertEquals(response.size(), 0);
    }

    private void assertGetFullCredentialsFailed() {
        boolean exception = false;
        try {
            List response = restTemplate.getForObject(
                    getRestAPIHostPort() + PLS_MARKETO_CREDENTIAL_URL, List.class);
        } catch (Exception e) {
            exception = true;
        }
        assertTrue(exception, "Should have thrown an exception");
    }

    private List<MarketoCredential> getMarketoCredentials(String endpoint) {
        List response = restTemplate.getForObject(
                getRestAPIHostPort() + endpoint, List.class);
        assertNotNull(response);
        List<MarketoCredential> credentialList = JsonUtils.convertList(response, MarketoCredential.class);
        assertEquals(response.size(), 1);
        return credentialList;
    }
    
    private MarketoCredential getMarketoCredential(Long credentialId) {
        MarketoCredential marketoCredential = restTemplate.getForObject(
                getRestAPIHostPort() + PLS_MARKETO_CREDENTIAL_URL + "/" + credentialId, MarketoCredential.class);
        assertNotNull(marketoCredential);
        return marketoCredential;
    }
    
    private List<ScoringRequestConfigSummary> getScoringRequestConfigs(Long credentialId) {
        List response = restTemplate.getForObject(
                getRestAPIHostPort() + PLS_MARKETO_CREDENTIAL_URL + "/" + credentialId + PLS_MARKETO_SCORING_REQUESTS, List.class);
        assertNotNull(response);
        List<ScoringRequestConfigSummary> scoringRequestConfigList = JsonUtils.convertList(response, ScoringRequestConfigSummary.class);
        return scoringRequestConfigList;
    }
    
    private ScoringRequestConfig getScoringRequestConfig(Long credentialId, String configId) {
        ScoringRequestConfig scoringRequestConfig = restTemplate.getForObject(
                getRestAPIHostPort() + PLS_MARKETO_CREDENTIAL_URL + "/" + credentialId + PLS_MARKETO_SCORING_REQUESTS + "/" + configId , ScoringRequestConfig.class);
        assertNotNull(scoringRequestConfig);
        return scoringRequestConfig;
    }
    
    private void assertMarketoCrendetial(MarketoCredential marketoCredential, String credName, boolean simplified) {
        assertNotNull(marketoCredential);
        
        assertEquals(marketoCredential.getName(), credName);
        assertEquals(marketoCredential.getSoapEndpoint(), SOAP_ENDPOINT);
        assertEquals(marketoCredential.getSoapUserId(), SOAP_USER_ID);
        assertEquals(marketoCredential.getSoapEncryptionKey(), SOAP_ENCRYPTION_KEY);
        assertEquals(marketoCredential.getRestEndpoint(), REST_ENDPOINT);
        assertEquals(marketoCredential.getRestIdentityEnpoint(), REST_IDENTITY_ENDPOINT);
        assertEquals(marketoCredential.getRestClientId(), REST_CLIENT_ID);
        assertEquals(marketoCredential.getRestClientSecret(), REST_CLIENT_SECRET);
        
        if (simplified) {
            assertNull(marketoCredential.getEnrichment());
            return;
        }
        
        assertNotNull(marketoCredential.getEnrichment());
        assertEquals(marketoCredential.getEnrichment().getWebhookUrl(), enrichmentWebhookUrl);
        assertEquals(marketoCredential.getEnrichment().getTenantCredentialGUID(),
                UuidUtils.packUuid(mainTestTenant.getId(), Long.toString(marketoCredential.getPid())));
    }
    
    
    
}
