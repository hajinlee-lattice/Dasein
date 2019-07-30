package com.latticeengines.pls.controller;

import static com.latticeengines.testframework.exposed.utils.MarketoConnectorHelper.REST_CLIENT_ID;
import static com.latticeengines.testframework.exposed.utils.MarketoConnectorHelper.REST_CLIENT_SECRET;
import static com.latticeengines.testframework.exposed.utils.MarketoConnectorHelper.REST_ENDPOINT;
import static com.latticeengines.testframework.exposed.utils.MarketoConnectorHelper.REST_IDENTITY_ENDPOINT;
import static com.latticeengines.testframework.exposed.utils.MarketoConnectorHelper.SOAP_ENCRYPTION_KEY;
import static com.latticeengines.testframework.exposed.utils.MarketoConnectorHelper.SOAP_ENDPOINT;
import static com.latticeengines.testframework.exposed.utils.MarketoConnectorHelper.SOAP_USER_ID;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.MarketoCredential;
import com.latticeengines.domain.exposed.pls.MarketoScoringMatchField;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfig;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfigContext;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfigSummary;
import com.latticeengines.domain.exposed.scoringapi.FieldInterpretation;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.pls.PlsInternalProxy;
import com.latticeengines.testframework.exposed.proxy.pls.PlsMarketoCredentialProxy;
import com.latticeengines.testframework.exposed.utils.MarketoConnectorHelper;

public class MarketoCredentialResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    @Value("${pls.marketo.enrichment.webhook.url}")
    private String enrichmentWebhookUrl;

    @Value("${pls.marketo.scoring.webhook.resource}")
    private String scoringWebhookResource;

    @Autowired
    private PlsInternalProxy plsInternalProxy;

    private static final String CREDENTIAL_NAME = "TEST-DP-MARKETO-SCORING-CONFIG-";

    @Inject
    private PlsMarketoCredentialProxy marketoCredProxy;

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
        MultiTenantContext.setTenant(mainTestTenant);
        attachProtectedProxy(marketoCredProxy);
    }

    /**
     * As most of the find operations performed on reader connection, we need to add some delay before making find call.
     * In real world scenario, this is consumed at UI layer, we can are fine with few milli-seconds of delay
     */
    private void addDelay() {
        try {
            Thread.sleep(2000L);
        } catch (InterruptedException e) {
            //Ignore
        }
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
        MarketoCredential marketoCredential = MarketoConnectorHelper.getTestMarketoCredentialConfig();
        String credName = CREDENTIAL_NAME + System.currentTimeMillis();
        marketoCredential.setName(credName);

        marketoCredProxy.createMarketoCredential(marketoCredential);

        List<MarketoCredential> credentialList = marketoCredProxy.getMarketoCredentialsSimplified();
        assertEquals(credentialList.size(), 1);
        marketoCredential = credentialList.get(0);
        assertMarketoCrendetial(marketoCredential, credName, true);

        credentialList = marketoCredProxy.getMarketoCredentials();
        assertEquals(credentialList.size(), 1);
        marketoCredential = credentialList.get(0);
        assertMarketoCrendetial(marketoCredential, credName, false);

        marketoCredentialId = marketoCredential.getPid();
        MarketoCredential marketoCredentialFromRest = marketoCredProxy.getMarketoCredential(marketoCredentialId);
        assertMarketoCrendetial(marketoCredentialFromRest, credName, false);
    }


    @Test(groups = {"deployment"}, dependsOnMethods = "createMarketoCredential_assertCreation")
    public void testCreateScoringRequestConfig_assertCreation() {
        MarketoCredential marketoCredential = marketoCredProxy.getMarketoCredential(marketoCredentialId);
        assertNotNull(marketoCredential);
        List<ScoringRequestConfigSummary> reqConfigSummaryLst = marketoCredProxy.getScoringRequestConfigs(marketoCredential.getPid());
        assertNotNull(reqConfigSummaryLst);
        assertEquals(reqConfigSummaryLst.size(), 0);

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

        ScoringRequestConfig createdReq = marketoCredProxy.createScoringRequestConfig(marketoCredential.getPid(), scoringReqConf);
        assertNotNull(createdReq);
        assertNotNull(createdReq.getConfigId());

        addDelay();
        reqConfigSummaryLst = marketoCredProxy.getScoringRequestConfigs(marketoCredential.getPid());
        assertNotNull(reqConfigSummaryLst);
        assertTrue(reqConfigSummaryLst.size() == 1);

        assertNotNull(reqConfigSummaryLst.get(0).getModelUuid());
        assertNotNull(reqConfigSummaryLst.get(0).getConfigId());
        assertEquals(reqConfigSummaryLst.get(0).getConfigId(), createdReq.getConfigId());

        ScoringRequestConfig scoringReqConfFromRest = marketoCredProxy.getScoringRequestConfig(marketoCredential.getPid(), reqConfigSummaryLst.get(0).getConfigId());
        assertNotNull(scoringReqConfFromRest);
        assertNotNull(scoringReqConfFromRest.getMarketoScoringMatchFields());
        assertEquals(scoringReqConfFromRest.getMarketoScoringMatchFields().size(), scoringMappings.size());
        assertEquals(scoringReqConfFromRest.getWebhookResource(), scoringWebhookResource);
    }

    @Test(groups = "deployment", dependsOnMethods = "testCreateScoringRequestConfig_assertCreation")
    public void testCreateScoringRequestConfigDuplicate_assertCreationFails() {
        MarketoCredential marketoCredential = marketoCredProxy.getMarketoCredential(marketoCredentialId);
        assertNotNull(marketoCredential);
        List<ScoringRequestConfigSummary> reqConfigSummaryLst = marketoCredProxy.getScoringRequestConfigs(marketoCredential.getPid());
        assertNotNull(reqConfigSummaryLst);
        assertEquals(reqConfigSummaryLst.size(), 1);

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
            marketoCredProxy.createScoringRequestConfig(marketoCredential.getPid(), scoringReqConf);
        } catch (Exception e) {
            exception = e;
        }

        assertNotNull(exception);
        assertTrue(exception.getMessage().contains(LedpException.buildMessage(LedpCode.LEDP_18192, new String[] {marketoCredential.getName()})));

        reqConfigSummaryLst = marketoCredProxy.getScoringRequestConfigs(marketoCredential.getPid());
        assertNotNull(reqConfigSummaryLst);
        assertEquals(reqConfigSummaryLst.size(), 1);
    }

    @Test(groups = "deployment", dependsOnMethods = "testCreateScoringRequestConfig_assertCreation")
    public void testFindMethods() {
        List<ScoringRequestConfigSummary> requestConfigLst = marketoCredProxy.getScoringRequestConfigs(marketoCredentialId);
        assertNotNull(requestConfigLst);
        assertEquals(requestConfigLst.size(), 1);
        ScoringRequestConfigSummary configSummary = requestConfigLst.get(0);
        assertNotNull(configSummary.getConfigId());
        assertNotNull(configSummary.getModelUuid());

        ScoringRequestConfig scoreReqConf2 = marketoCredProxy.getScoringRequestConfig(marketoCredentialId, configSummary.getConfigId());
        assertNotNull(scoreReqConf2);
        assertNotNull(scoreReqConf2.getMarketoScoringMatchFields());
        assertEquals(scoreReqConf2.getWebhookResource(), scoringWebhookResource);
    }

    @Test(groups = "deployment", dependsOnMethods = "testCreateScoringRequestConfig_assertCreation")
    public void testRetrieveMethods_assertScoringRequestConfigContext() {
        List<ScoringRequestConfigSummary> requestConfigLst = marketoCredProxy.getScoringRequestConfigs(marketoCredentialId);
        assertNotNull(requestConfigLst);
        assertEquals(requestConfigLst.size(), 1);
        ScoringRequestConfigSummary configSummary = requestConfigLst.get(0);
        assertNotNull(configSummary.getConfigId());
        assertNotNull(configSummary.getModelUuid());

        ScoringRequestConfigContext srcContext = plsInternalProxy.retrieveScoringRequestConfigContext(configSummary.getConfigId());
        assertNotNull(srcContext);
        assertNotNull(srcContext.getConfigId());
        assertNotNull(srcContext.getSecretKey());
        assertEquals(srcContext.getConfigId(), configSummary.getConfigId());
        assertEquals(srcContext.getModelUuid(), configSummary.getModelUuid());
        assertEquals(srcContext.getTenantId(), mainTestTenant.getId());
        assertTrue(srcContext.getExternalProfileId().contains(marketoCredentialId.toString()));

        // Check with Invalid ID
        Exception exception = null;
        try {
            srcContext = plsInternalProxy.retrieveScoringRequestConfigContext("DummyId");
        } catch (Exception e) {
            exception = e;
        }
        assertNotNull(exception);

        assertTrue(exception.getMessage().contains(LedpCode.LEDP_18194.toString()));

        // Check with Empty ID
        exception = null;
        try {
            srcContext = plsInternalProxy.retrieveScoringRequestConfigContext(null);
        } catch (Exception e) {
            exception = e;
        }
        assertNotNull(exception);
        assertTrue(exception.getMessage().contains(LedpCode.LEDP_18194.toString()));
    }

    @Test(groups = "deployment", dependsOnMethods = "testFindMethods")
    public void testUpdateScoringRequestConfig_assertFieldAdditions() {
        List<ScoringRequestConfigSummary> requestConfigLst = marketoCredProxy.getScoringRequestConfigs(marketoCredentialId);
        ScoringRequestConfigSummary configSummary = requestConfigLst.get(0);
        ScoringRequestConfig scoreReqConf1 = marketoCredProxy.getScoringRequestConfig(marketoCredentialId, configSummary.getConfigId());
        assertNotNull(scoreReqConf1);

        MARKETO_SCORE_MATCH_FIELD_4.setModelFieldName(FieldInterpretation.DUNS.toString());
        MARKETO_SCORE_MATCH_FIELD_4.setMarketoFieldName("lead.DUNS");

        MARKETO_SCORE_MATCH_FIELD_5.setModelFieldName(FieldInterpretation.Domain.toString());
        MARKETO_SCORE_MATCH_FIELD_5.setMarketoFieldName("lead.Domain");

        List<MarketoScoringMatchField> updatedMatchFields = new ArrayList<>(scoreReqConf1.getMarketoScoringMatchFields());
        updatedMatchFields.add(MARKETO_SCORE_MATCH_FIELD_4);
        updatedMatchFields.add(MARKETO_SCORE_MATCH_FIELD_5);
        scoreReqConf1.setMarketoScoringMatchFields(updatedMatchFields);

        marketoCredProxy.updateScoringRequestConfig(marketoCredentialId, scoreReqConf1);
        addDelay();
        ScoringRequestConfig scoreReqConf2 = marketoCredProxy.getScoringRequestConfig(marketoCredentialId, scoreReqConf1.getConfigId());
        assertNotNull(scoreReqConf2);

        assertEquals(scoreReqConf2.getMarketoScoringMatchFields().size(), updatedMatchFields.size());
    }

    @Test(groups = "deployment", dependsOnMethods = "testUpdateScoringRequestConfig_assertFieldAdditions")
    public void testUpdateScoringRequestConfig_assertFieldRemoval() {
        List<ScoringRequestConfigSummary> requestConfigLst = marketoCredProxy.getScoringRequestConfigs(marketoCredentialId);
        ScoringRequestConfigSummary configSummary = requestConfigLst.get(0);
        ScoringRequestConfig scoreReqConf1 = marketoCredProxy.getScoringRequestConfig(marketoCredentialId, configSummary.getConfigId());
        assertNotNull(scoreReqConf1);

        List<MarketoScoringMatchField> updatedMatchFields = scoreReqConf1.getMarketoScoringMatchFields().subList(0, 3);
        scoreReqConf1.setMarketoScoringMatchFields(updatedMatchFields);

        marketoCredProxy.updateScoringRequestConfig(marketoCredentialId, scoreReqConf1);
        addDelay();
        ScoringRequestConfig scoreReqConf2 = marketoCredProxy.getScoringRequestConfig(marketoCredentialId, scoreReqConf1.getConfigId());
        assertNotNull(scoreReqConf2);

        assertEquals(scoreReqConf2.getMarketoScoringMatchFields().size(), updatedMatchFields.size());
    }

    @Test(groups = "deployment", dependsOnMethods = "testUpdateScoringRequestConfig_assertFieldRemoval")
    public void testUpdateScoringRequestConfig_assertFieldsAddAndUpdate() {
        List<ScoringRequestConfigSummary> requestConfigLst = marketoCredProxy.getScoringRequestConfigs(marketoCredentialId);
        ScoringRequestConfigSummary configSummary = requestConfigLst.get(0);
        ScoringRequestConfig scoreReqConf1 = marketoCredProxy.getScoringRequestConfig(marketoCredentialId, configSummary.getConfigId());
        assertNotNull(scoreReqConf1);

        MARKETO_SCORE_MATCH_FIELD_6.setModelFieldName(FieldInterpretation.Industry.toString());
        MARKETO_SCORE_MATCH_FIELD_6.setMarketoFieldName("lead.Industry");

        List<MarketoScoringMatchField> updatedMatchFields = new ArrayList<>(scoreReqConf1.getMarketoScoringMatchFields());
        updatedMatchFields.add(MARKETO_SCORE_MATCH_FIELD_6);
        scoreReqConf1.setMarketoScoringMatchFields(updatedMatchFields);

        updatedMatchFields.forEach(matchField -> matchField.setMarketoFieldName(matchField.getMarketoFieldName() + "-Updated"));

        marketoCredProxy.updateScoringRequestConfig(marketoCredentialId, scoreReqConf1);
        addDelay();
        ScoringRequestConfig scoreReqConf2 = marketoCredProxy.getScoringRequestConfig(marketoCredentialId, scoreReqConf1.getConfigId());
        assertNotNull(scoreReqConf2);

        assertEquals(scoreReqConf2.getMarketoScoringMatchFields().size(), updatedMatchFields.size());
        scoreReqConf2.getMarketoScoringMatchFields().forEach(matchField -> assertTrue(matchField.getMarketoFieldName().endsWith("-Updated")));
    }

    private void assertGetSimplifiedCredentialsSuccess() {
        List response = restTemplate.getForObject(
                getRestAPIHostPort() + PlsMarketoCredentialProxy.PLS_MARKETO_CREDENTIAL_URL + "/simplified", List.class);
        assertNotNull(response);
        assertEquals(response.size(), 0);
    }

    private void assertGetFullCredentialsSuccess() {
        List response = restTemplate.getForObject(
                getRestAPIHostPort() + PlsMarketoCredentialProxy.PLS_MARKETO_CREDENTIAL_URL, List.class);
        assertNotNull(response);
        assertEquals(response.size(), 0);
    }

    private void assertGetFullCredentialsFailed() {
        boolean exception = false;
        try {
            List response = restTemplate.getForObject(
                    getRestAPIHostPort() + PlsMarketoCredentialProxy.PLS_MARKETO_CREDENTIAL_URL, List.class);
        } catch (Exception e) {
            exception = true;
        }
        assertTrue(exception, "Should have thrown an exception");
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
        assertNotNull(marketoCredential.getLatticeSecretKey());
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
