package com.latticeengines.scoringapi.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.apache.log4j.Logger;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.client.RestTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.domain.exposed.exception.ExceptionHandlerErrors;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.BucketName;
import com.latticeengines.domain.exposed.pls.MarketoCredential;
import com.latticeengines.domain.exposed.pls.MarketoScoringMatchField;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfig;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfigSummary;
import com.latticeengines.domain.exposed.scoringapi.FieldInterpretation;
import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.ScoreResponse;
import com.latticeengines.scoringapi.functionalframework.ScoringApiControllerDeploymentTestNGBase;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.testframework.exposed.proxy.pls.PlsMarketoCredentialProxy;
import com.latticeengines.testframework.exposed.utils.MarketoConnectorHelper;
import com.latticeengines.transform.v2_0_25.common.JsonUtils;

public class ScoreExternalResourceDeploymentTestNG extends ScoringApiControllerDeploymentTestNGBase {

    private static Logger LOG = Logger.getLogger(ScoreExternalResourceDeploymentTestNG.class);

    private static final String MARKETO_CRED_NAME = "TestProfileFromScoringAPI";

    private static final MarketoScoringMatchField MARKETO_SCORE_MATCH_FIELD_1 = new MarketoScoringMatchField();
    private static final MarketoScoringMatchField MARKETO_SCORE_MATCH_FIELD_2 = new MarketoScoringMatchField();
    private static final MarketoScoringMatchField MARKETO_SCORE_MATCH_FIELD_3 = new MarketoScoringMatchField();

    private static final int EXPECTED_SCORE_99 = 99;
    private static final int EXPECTED_SCORE_67 = 67;

    @Inject
    private PlsMarketoCredentialProxy marketoCredProxy;

    private RestTemplate restTemplate;
    private MarketoCredential marketoCredential;
    private ScoringRequestConfigSummary scoringRequestConfigSummary;

    @BeforeClass(groups = "deployment")
    public void beforeClass() throws IOException {
        super.beforeClass();
        deploymentTestBed.attachProtectedProxy(marketoCredProxy);
        restTemplate = HttpClientUtils.newRestTemplate();
    }

    @Test(groups="deployment")
    public void testCreateMarketoCredential() {
        // Setup Marketo Profile and Webhook request
        MarketoCredential marketoCred = MarketoConnectorHelper.getTestMarketoCredentialConfig();
        marketoCred.setName(MARKETO_CRED_NAME+System.currentTimeMillis());
        marketoCredProxy.createMarketoCredential(marketoCred);
        List<MarketoCredential> marketoCreds = marketoCredProxy.getMarketoCredentials();
        assertEquals(marketoCreds.size(), 1);
        assertNotNull(marketoCreds.get(0), "Marketo Credential is null");
        marketoCredential = marketoCreds.get(0);
    }

    @Test(groups="deployment")
    public void testVerifyModelSummary() {
        List<ModelSummary> modelSummaries = modelSummaryProxy.findPaginatedModels(customerSpace.toString(),
                null, true, 0, 10);
        assertNotNull(modelSummaries, "Model Summaries were not initialized");
        assertEquals(modelSummaries.size(), 1);
        assertNotNull(modelSummaries.get(0).getId());
        assertEquals(modelSummaries.get(0).getId(), MODEL_ID);
    }

    @Test(groups="deployment", dependsOnMethods = {"testCreateMarketoCredential", "testVerifyModelSummary"})
    public void testCreateScoringRequestConfig() {
        List<ScoringRequestConfigSummary> reqConfigSummaryLst = marketoCredProxy.getScoringRequestConfigs(marketoCredential.getPid());
        assertNotNull(reqConfigSummaryLst);
        assertTrue(reqConfigSummaryLst.size() == 0);

        ScoringRequestConfig scoringReqConf = new ScoringRequestConfig();
        scoringReqConf.setMarketoCredential(marketoCredential);
        scoringReqConf.setModelUuid(MODEL_ID);

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
        scoringRequestConfigSummary = reqConfigSummaryLst.get(0);
    }

    @Test(groups="deployment", dependsOnMethods = {"testCreateScoringRequestConfig"})
    public void testScoreRecord_assertSuccess() throws IOException {
        String url = apiHostPort + "/score/external/record/" + scoringRequestConfigSummary.getConfigId();
        LOG.info("Calling Scoring API at: " + url);
        ScoreRequest scoreRequest = getScoreRequest();
        scoreRequest.setModelId(MODEL_ID);
        HttpHeaders headers = new HttpHeaders();
        headers.set(Constants.LATTICE_SECRET_KEY_HEADERNAME, marketoCredential.getLatticeSecretKey());
        HttpEntity<ScoreRequest> entityReq = new HttpEntity<ScoreRequest>(scoreRequest, headers);

        ResponseEntity<ScoreResponse> response = null;
        Exception exception = null;
        ExceptionHandlerErrors exceptionDetails = null;
        try {
            response = restTemplate.postForEntity(url, entityReq, ScoreResponse.class);
        } catch (RestClientResponseException e) {
            exception = e;
            exceptionDetails = JsonUtils.deserialize(e.getResponseBodyAsString(), ExceptionHandlerErrors.class);
        }
        assertNull(exception);
        assertNull(exceptionDetails);
        ScoreResponse scoreResponse = response.getBody();
        Assert.assertEquals(scoreResponse.getScore(), EXPECTED_SCORE_67);
        Assert.assertNotNull(scoreResponse.getBucket());
        Assert.assertEquals(scoreResponse.getBucket(), BucketName.C.toValue());
    }

    @Test(groups="deployment", dependsOnMethods = {"testCreateScoringRequestConfig"})
    public void testScoreRecord_assertFailWithMissingSecretKey() throws IOException {
        String url = apiHostPort + "/score/external/record/" + scoringRequestConfigSummary.getConfigId();
        LOG.info("Calling Scoring API at: " + url);
        ScoreRequest scoreRequest = getScoreRequest();
        scoreRequest.setModelId(MODEL_ID);
        HttpHeaders headers = new HttpHeaders();
        HttpEntity<ScoreRequest> entityReq = new HttpEntity<ScoreRequest>(scoreRequest, headers);
        Exception exception = null;
        ExceptionHandlerErrors exceptionDetails = null;
        try {
            restTemplate.postForEntity(url, entityReq, ScoreResponse.class);
        } catch (RestClientResponseException e) {
            exception = e;
            exceptionDetails = JsonUtils.deserialize(e.getResponseBodyAsString(), ExceptionHandlerErrors.class);
        }
        assertNotNull(exception);
        assertNotNull(exceptionDetails);
        assertTrue(exceptionDetails.getDescription().contains(LedpCode.LEDP_18199.getMessage()));
    }

    @Test(groups="deployment", dependsOnMethods = {"testCreateScoringRequestConfig"})
    public void testScoreRecord_assertFailWithInvalidSecretKey() throws IOException {
        String url = apiHostPort + "/score/external/record/" + scoringRequestConfigSummary.getConfigId();
        LOG.info("Calling Scoring API at: " + url);
        ScoreRequest scoreRequest = getScoreRequest();
        scoreRequest.setModelId(MODEL_ID);
        HttpHeaders headers = new HttpHeaders();
        headers.set(Constants.LATTICE_SECRET_KEY_HEADERNAME, "Invalid Key");
        HttpEntity<ScoreRequest> entityReq = new HttpEntity<ScoreRequest>(scoreRequest, headers);

        Exception exception = null;
        ExceptionHandlerErrors exceptionDetails = null;
        try {
            restTemplate.postForEntity(url, entityReq, ScoreResponse.class);
        } catch (RestClientResponseException e) {
            exception = e;
            exceptionDetails = JsonUtils.deserialize(e.getResponseBodyAsString(), ExceptionHandlerErrors.class);
        }
        assertNotNull(exception);
        assertNotNull(exceptionDetails);
        assertTrue(exceptionDetails.getDescription().contains(LedpCode.LEDP_18200.getMessage()));
    }

    @Test(groups="deployment", dependsOnMethods = {"testCreateScoringRequestConfig"})
    public void testScoreRecord_assertFailWithMissingRecord() throws IOException {
        String url = apiHostPort + "/score/external/record/" + scoringRequestConfigSummary.getConfigId();
        LOG.info("Calling Scoring API at: " + url);
        ScoreRequest scoreRequest = new ScoreRequest();
        scoreRequest.setModelId(MODEL_ID);
        HttpHeaders headers = new HttpHeaders();
        headers.set(Constants.LATTICE_SECRET_KEY_HEADERNAME, marketoCredential.getLatticeSecretKey());
        HttpEntity<ScoreRequest> entityReq = new HttpEntity<ScoreRequest>(scoreRequest, headers);

        Exception exception = null;
        ExceptionHandlerErrors exceptionDetails = null;
        try {
            restTemplate.postForEntity(url, entityReq, ScoreResponse.class);
        } catch (RestClientResponseException e) {
            exception = e;
            exceptionDetails = JsonUtils.deserialize(e.getResponseBodyAsString(), ExceptionHandlerErrors.class);
        }
        assertNotNull(exception);
        assertNotNull(exceptionDetails);
        assertTrue(exceptionDetails.getDescription().contains(LedpCode.LEDP_18201.getMessage()));
    }

    @Test(groups="deployment", dependsOnMethods = {"testCreateScoringRequestConfig"})
    public void testScoreRecord_assertFailWithInvalidRequestId() throws IOException {
        final String invalidConfigId = "InvalidRequestID";
        String url = apiHostPort + "/score/external/record/" + invalidConfigId;
        LOG.info("Calling Scoring API at: " + url);
        ScoreRequest scoreRequest = getScoreRequest();
        scoreRequest.setModelId(MODEL_ID);
        HttpHeaders headers = new HttpHeaders();
        headers.set(Constants.LATTICE_SECRET_KEY_HEADERNAME, marketoCredential.getLatticeSecretKey());
        HttpEntity<ScoreRequest> entityReq = new HttpEntity<ScoreRequest>(scoreRequest, headers);

        Exception exception = null;
        ExceptionHandlerErrors exceptionDetails = null;
        try {
            restTemplate.postForEntity(url, entityReq, ScoreResponse.class);
        } catch (RestClientResponseException e) {
            exception = e;
            exceptionDetails = JsonUtils.deserialize(e.getResponseBodyAsString(), ExceptionHandlerErrors.class);
        }
        assertNotNull(exception);
        assertNotNull(exceptionDetails);
        LOG.error(JsonUtils.serialize(exceptionDetails));
        assertTrue(exceptionDetails.getDescription().contains(LedpException.buildMessage(LedpCode.LEDP_18194, new String[] {invalidConfigId})));
    }

    /**
     * As ScoringRequestConfig find operations performed on reader connection, we need to add some delay before making find call.
     * In real world scenario, this is consumed at UI layer, we can are fine with few milli-seconds of delay
     */
    private void addDelay() {
        try {
            Thread.sleep(2000L);
        } catch (InterruptedException e) {
            //Ignore
        }
    }
}
