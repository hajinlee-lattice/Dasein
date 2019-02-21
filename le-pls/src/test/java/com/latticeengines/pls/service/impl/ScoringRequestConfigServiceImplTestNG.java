package com.latticeengines.pls.service.impl;

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

import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.MarketoCredential;
import com.latticeengines.domain.exposed.pls.MarketoScoringMatchField;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfig;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfigContext;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfigSummary;
import com.latticeengines.domain.exposed.scoringapi.FieldInterpretation;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.MarketoCredentialService;
import com.latticeengines.pls.service.ScoringRequestConfigService;
import com.latticeengines.security.exposed.service.TenantService;

public class ScoringRequestConfigServiceImplTestNG extends PlsFunctionalTestNGBase {

    private static String CREDENTIAL_NAME = "TEST-MARKETO-SCORING-CONFIG-";

    private static final String TENANT1 = "TENANT1";
    private static final String SOAP_ENDPOINT = "https://948-IYP-205.mktoapi.com/soap/mktows/2_9";
    private static final String SOAP_USER_ID = "latticeengines1_511435204E14C09D06A6E8";
    private static final String SOAP_ENCRYPTION_KEY = "140990042468919944EE1144CC0099EF0066CF0EE494";
    private static final String REST_ENDPOINT = "https://948-IYP-205.mktorest.com/rest";
    private static final String REST_IDENTITY_ENDPOINT = "https://948-IYP-205.mktorest.com/identity";
    private static final String REST_CLIENT_ID = "dafede33-f785-48d1-85fa-b6ebdb884d06";
    private static final String REST_CLIENT_SECRET = "1R0LCTlmNd7G2PGh9ZJj8SIKSjEVZ8Ik";

    // Scoring Constants
    private static final String MODEL_UUID = "ms__0f4217c2-f234-443a-af42-6d7b7a7ff9f3-PLSModel";

    private static final MarketoScoringMatchField MARKETO_SCORE_MATCH_FIELD_1 = new MarketoScoringMatchField();
    private static final MarketoScoringMatchField MARKETO_SCORE_MATCH_FIELD_2 = new MarketoScoringMatchField();
    private static final MarketoScoringMatchField MARKETO_SCORE_MATCH_FIELD_3 = new MarketoScoringMatchField();
    private static final MarketoScoringMatchField MARKETO_SCORE_MATCH_FIELD_4 = new MarketoScoringMatchField();
    private static final MarketoScoringMatchField MARKETO_SCORE_MATCH_FIELD_5 = new MarketoScoringMatchField();
    private static final MarketoScoringMatchField MARKETO_SCORE_MATCH_FIELD_6 = new MarketoScoringMatchField();

    @Autowired
    private MarketoCredentialService marketoCredentialService;

    @Autowired
    private ScoringRequestConfigService scoringRequestConfigService;

    @Autowired
    private TenantService tenantService;

    @Value("${pls.marketo.enrichment.webhook.url}")
    private String enrichmentWebhookUrl;

    @Value("${pls.marketo.scoring.webhook.resource}")
    private String scoringWebhookResource;

    private Long marketoCredentialId = null;

    private void setupTenant(String t) {
        Tenant tenant = tenantService.findByTenantId(t);
        if (tenant != null) {
            tenantService.discardTenant(tenant);
        }
        tenant = new Tenant();
        tenant.setId(t);
        tenant.setName(t);
        tenantService.registerTenant(tenant);

        setupSecurityContext(tenant);
    }

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        setupTenant(TENANT1);
        CREDENTIAL_NAME += System.currentTimeMillis();
    }


    /**
     * As most of the find operations performed on reader connection, we need to add some delay before making find call.
     * In real world scenario, this is consumed at UI layer, we can are fine with few milli-seconds of delay
     */
    private void addDelay() {
        try {
            Thread.sleep(2000L);
        } catch (InterruptedException e) {
            logger.warn("Interrupted", e);
        }
    }

    @Test(groups = "functional")
    public void createMarketoCredential_assertCredentialCreated() {
        MarketoCredential marketoCredential = new MarketoCredential();
        marketoCredential.setName(CREDENTIAL_NAME);
        marketoCredential.setSoapEndpoint(SOAP_ENDPOINT);
        marketoCredential.setSoapUserId(SOAP_USER_ID);
        marketoCredential.setSoapEncryptionKey(SOAP_ENCRYPTION_KEY);
        marketoCredential.setRestEndpoint(REST_ENDPOINT);
        marketoCredential.setRestIdentityEnpoint(REST_IDENTITY_ENDPOINT);
        marketoCredential.setRestClientId(REST_CLIENT_ID);
        marketoCredential.setRestClientSecret(REST_CLIENT_SECRET);

        marketoCredentialService.createMarketoCredential(marketoCredential);
        marketoCredentialId = marketoCredential.getPid();

        marketoCredential = marketoCredentialService.findMarketoCredentialById(marketoCredentialId.toString());
        assertNotNull(marketoCredential);

        assertEquals(marketoCredential.getName(), CREDENTIAL_NAME);
        assertEquals(marketoCredential.getSoapEndpoint(), SOAP_ENDPOINT);
        assertEquals(marketoCredential.getSoapUserId(), SOAP_USER_ID);
        assertEquals(marketoCredential.getSoapEncryptionKey(), SOAP_ENCRYPTION_KEY);
        assertEquals(marketoCredential.getRestEndpoint(), REST_ENDPOINT);
        assertEquals(marketoCredential.getRestIdentityEnpoint(), REST_IDENTITY_ENDPOINT);
        assertEquals(marketoCredential.getRestClientId(), REST_CLIENT_ID);
        assertEquals(marketoCredential.getRestClientSecret(), REST_CLIENT_SECRET);
        assertNotNull(marketoCredential.getLatticeSecretKey());
        assertNotNull(marketoCredential.getEnrichment());
        assertEquals(marketoCredential.getEnrichment().getWebhookUrl(), enrichmentWebhookUrl);
        assertEquals(marketoCredential.getEnrichment().getTenantCredentialGUID(),
                UuidUtils.packUuid(TENANT1, Long.toString(marketoCredential.getPid())));
    }

    @Test(groups = "functional", dependsOnMethods = "createMarketoCredential_assertCredentialCreated")
    public void testCreateScoringRequestConfig_assertCreation() {
        MarketoCredential marketoCredential = marketoCredentialService.findAllMarketoCredentials().get(0);
        assertNotNull(marketoCredential);
        List<ScoringRequestConfigSummary> reqConfigSummaryLst = scoringRequestConfigService
                .findAllByMarketoCredential(marketoCredential.getPid());
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

        scoringRequestConfigService.createScoringRequestConfig(scoringReqConf);
        assertNotNull(scoringReqConf.getPid());
        assertNotNull(scoringReqConf.getConfigId());
        addDelay();
        ScoringRequestConfig scoringReqConfFromDB = scoringRequestConfigService.findByConfigId(marketoCredential.getPid(), scoringReqConf.getConfigId());
        assertNotNull(scoringReqConfFromDB);
        assertNotNull(scoringReqConfFromDB.getMarketoScoringMatchFields());
        assertEquals(scoringReqConfFromDB.getPid(), scoringReqConf.getPid());
        assertEquals(scoringReqConfFromDB.getMarketoScoringMatchFields().size(), scoringMappings.size());
    }

    @Test(groups = "functional", dependsOnMethods = "testCreateScoringRequestConfig_assertCreation")
    public void testCreateScoringRequestConfigDuplicate_assertCreationFails() {
        MarketoCredential marketoCredential = marketoCredentialService.findAllMarketoCredentials().get(0);
        assertNotNull(marketoCredential);
        List<ScoringRequestConfigSummary> reqConfigSummaryLst = scoringRequestConfigService
                .findAllByMarketoCredential(marketoCredential.getPid());
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

        LedpException exception = null;
        try {
            scoringRequestConfigService.createScoringRequestConfig(scoringReqConf);
        } catch (LedpException e) {
            exception = e;
        }

        assertNotNull(exception);
        assertEquals(exception.getCode(), LedpCode.LEDP_18192);
        assertEquals(exception.getMessage(), LedpException.buildMessage(LedpCode.LEDP_18192, new String[] {marketoCredential.getName()}));

        reqConfigSummaryLst = scoringRequestConfigService
                .findAllByMarketoCredential(marketoCredential.getPid());
        assertNotNull(reqConfigSummaryLst);
        assertEquals(reqConfigSummaryLst.size(), 1);
    }

    @Test(groups = "functional", dependsOnMethods = "testCreateScoringRequestConfig_assertCreation")
    public void testFindMethods() {
        List<ScoringRequestConfigSummary> requestConfigLst =
                scoringRequestConfigService.findAllByMarketoCredential(marketoCredentialId);
        assertNotNull(requestConfigLst);
        assertEquals(requestConfigLst.size(), 1);
        ScoringRequestConfigSummary configSummary = requestConfigLst.get(0);
        assertNotNull(configSummary.getConfigId());
        assertNotNull(configSummary.getModelUuid());

        ScoringRequestConfig scoreReqConf1 = scoringRequestConfigService.findByModelUuid(marketoCredentialId, configSummary.getModelUuid());
        assertNotNull(scoreReqConf1);
        assertNotNull(scoreReqConf1.getMarketoScoringMatchFields());
        assertEquals(scoreReqConf1.getWebhookResource(), scoringWebhookResource);

        ScoringRequestConfig scoreReqConf2 = scoringRequestConfigService.findByConfigId(marketoCredentialId, configSummary.getConfigId());
        assertNotNull(scoreReqConf2);
        assertNotNull(scoreReqConf2.getMarketoScoringMatchFields());
        assertEquals(scoreReqConf2.getWebhookResource(), scoringWebhookResource);

        assertEquals(scoreReqConf2.getMarketoScoringMatchFields().size(), scoreReqConf1.getMarketoScoringMatchFields().size());
    }

    @Test(groups = "functional", dependsOnMethods = "testCreateScoringRequestConfig_assertCreation")
    public void testRetrieveMethods_assertScoringRequestConfigContext() {
        List<ScoringRequestConfigSummary> requestConfigLst =
                scoringRequestConfigService.findAllByMarketoCredential(marketoCredentialId);
        assertNotNull(requestConfigLst);
        assertEquals(requestConfigLst.size(), 1);
        ScoringRequestConfigSummary configSummary = requestConfigLst.get(0);
        assertNotNull(configSummary.getConfigId());
        assertNotNull(configSummary.getModelUuid());

        ScoringRequestConfigContext srcContext = scoringRequestConfigService.retrieveScoringRequestConfigContext(configSummary.getConfigId());
        assertNotNull(srcContext);
        assertNotNull(srcContext.getConfigId());
        assertNotNull(srcContext.getSecretKey());
        assertEquals(srcContext.getConfigId(), configSummary.getConfigId());
        assertEquals(srcContext.getModelUuid(), configSummary.getModelUuid());
        assertEquals(srcContext.getTenantId(), TENANT1);
        assertTrue(srcContext.getExternalProfileId().contains(marketoCredentialId.toString()));

        srcContext = scoringRequestConfigService.retrieveScoringRequestConfigContext("DummyId");
        assertNull(srcContext);
    }

    @Test(groups = "functional", dependsOnMethods = "testFindMethods")
    public void testUpdateScoringRequestConfig_assertFieldAdditions() {
        List<ScoringRequestConfigSummary> requestConfigLst =
                scoringRequestConfigService.findAllByMarketoCredential(marketoCredentialId);
        ScoringRequestConfigSummary configSummary = requestConfigLst.get(0);
        ScoringRequestConfig scoreReqConf1 = scoringRequestConfigService.findByConfigId(marketoCredentialId, configSummary.getConfigId());
        assertNotNull(scoreReqConf1);

        MARKETO_SCORE_MATCH_FIELD_4.setModelFieldName(FieldInterpretation.DUNS.toString());
        MARKETO_SCORE_MATCH_FIELD_4.setMarketoFieldName("lead.DUNS");

        MARKETO_SCORE_MATCH_FIELD_5.setModelFieldName(FieldInterpretation.Domain.toString());
        MARKETO_SCORE_MATCH_FIELD_5.setMarketoFieldName("lead.Domain");

        List<MarketoScoringMatchField> updatedMatchFields = new ArrayList<>(scoreReqConf1.getMarketoScoringMatchFields());
        updatedMatchFields.add(MARKETO_SCORE_MATCH_FIELD_4);
        updatedMatchFields.add(MARKETO_SCORE_MATCH_FIELD_5);
        scoreReqConf1.setMarketoScoringMatchFields(updatedMatchFields);

        scoringRequestConfigService.updateScoringRequestConfig(scoreReqConf1);
        addDelay();
        ScoringRequestConfig scoreReqConf2 = scoringRequestConfigService.findByConfigId(marketoCredentialId, scoreReqConf1.getConfigId());
        assertNotNull(scoreReqConf2);

        assertEquals(scoreReqConf2.getMarketoScoringMatchFields().size(), updatedMatchFields.size());
    }

    @Test(groups = "functional", dependsOnMethods = "testUpdateScoringRequestConfig_assertFieldAdditions")
    public void testUpdateScoringRequestConfig_assertFieldRemoval() {
        List<ScoringRequestConfigSummary> requestConfigLst =
                scoringRequestConfigService.findAllByMarketoCredential(marketoCredentialId);
        ScoringRequestConfigSummary configSummary = requestConfigLst.get(0);
        ScoringRequestConfig scoreReqConf1 = scoringRequestConfigService.findByConfigId(marketoCredentialId, configSummary.getConfigId());
        assertNotNull(scoreReqConf1);

        List<MarketoScoringMatchField> updatedMatchFields = scoreReqConf1.getMarketoScoringMatchFields().subList(0, 3);
        scoreReqConf1.setMarketoScoringMatchFields(updatedMatchFields);

        scoringRequestConfigService.updateScoringRequestConfig(scoreReqConf1);
        addDelay();
        ScoringRequestConfig scoreReqConf2 = scoringRequestConfigService.findByConfigId(marketoCredentialId, scoreReqConf1.getConfigId());
        assertNotNull(scoreReqConf2);

        assertEquals(scoreReqConf2.getMarketoScoringMatchFields().size(), updatedMatchFields.size());
    }

    @Test(groups = "functional", dependsOnMethods = "testUpdateScoringRequestConfig_assertFieldRemoval")
    public void testUpdateScoringRequestConfig_assertFieldsAddAndUpdate() {
        List<ScoringRequestConfigSummary> requestConfigLst =
                scoringRequestConfigService.findAllByMarketoCredential(marketoCredentialId);
        ScoringRequestConfigSummary configSummary = requestConfigLst.get(0);
        ScoringRequestConfig scoreReqConf1 = scoringRequestConfigService.findByConfigId(marketoCredentialId, configSummary.getConfigId());
        assertNotNull(scoreReqConf1);

        MARKETO_SCORE_MATCH_FIELD_6.setModelFieldName(FieldInterpretation.Industry.toString());
        MARKETO_SCORE_MATCH_FIELD_6.setMarketoFieldName("lead.Industry");

        List<MarketoScoringMatchField> updatedMatchFields = new ArrayList<>(scoreReqConf1.getMarketoScoringMatchFields());
        updatedMatchFields.add(MARKETO_SCORE_MATCH_FIELD_6);
        scoreReqConf1.setMarketoScoringMatchFields(updatedMatchFields);

        updatedMatchFields.forEach(matchField -> matchField.setMarketoFieldName(matchField.getMarketoFieldName() + "-Updated"));

        scoringRequestConfigService.updateScoringRequestConfig(scoreReqConf1);
        addDelay();
        ScoringRequestConfig scoreReqConf2 = scoringRequestConfigService.findByConfigId(marketoCredentialId, scoreReqConf1.getConfigId());
        assertNotNull(scoreReqConf2);

        assertEquals(scoreReqConf2.getMarketoScoringMatchFields().size(), updatedMatchFields.size());
        scoreReqConf2.getMarketoScoringMatchFields().forEach(matchField -> assertTrue(matchField.getMarketoFieldName().endsWith("-Updated")));
    }

    @Test(groups = "functional", dependsOnMethods = "testUpdateScoringRequestConfig_assertFieldsAddAndUpdate")
    public void testDeleteMarketoCredential_assertDeleteScoringRequests() {
        marketoCredentialService.deleteMarketoCredentialById(marketoCredentialId.toString());
        addDelay();
        MarketoCredential marketoCredential = marketoCredentialService.findMarketoCredentialById(marketoCredentialId.toString());
        assertNull(marketoCredential);
        List<ScoringRequestConfigSummary> requestConfigLst = scoringRequestConfigService.findAllByMarketoCredential(marketoCredentialId);
        assertNotNull(requestConfigLst);
        assertTrue(requestConfigLst.isEmpty());
    }
}
