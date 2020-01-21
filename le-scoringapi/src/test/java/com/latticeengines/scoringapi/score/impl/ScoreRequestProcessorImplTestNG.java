package com.latticeengines.scoringapi.score.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.rest.HttpStopWatch;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.scoringapi.BulkRecordScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.Warnings;
import com.latticeengines.scoringapi.exposed.ScoringArtifacts;
import com.latticeengines.scoringapi.exposed.context.RequestInfo;
import com.latticeengines.scoringapi.exposed.model.ModelRetriever;
import com.latticeengines.scoringapi.functionalframework.ScoringApiFunctionalTestNGBase;
import com.latticeengines.scoringapi.score.AdditionalScoreConfig;
import com.latticeengines.scoringapi.score.ScoreRequestProcessor;
public class ScoreRequestProcessorImplTestNG extends ScoringApiFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ScoreRequestProcessorImplTestNG.class);

    private CustomerSpace space = CustomerSpace.parse("tenant");

    private String requestId = "426a88c6-f158-4b94-bd41-b1f03905c991";

    private String modelId = "ms__cd1fa727-b32b-47bb-b622-8455ef2e1ff7-PLSModel";

    private static final String requestStr = "{\n  \"source\": \"Salesforce\",\n  \"records\": [\n    {\n      \"rootOperationId\": null,\n      \"requestTimestamp\": null,\n      \"recordId\": \"00Q1k0000011gx9EAA\",\n      \"idType\": \"SFDC\",\n      \"modelAttributeValuesMap\": {\n        \"ms__cd1fa727-b32b-47bb-b622-8455ef2e1ff7-PLSModel\": {\n          \"DUNS\": null,\n          \"CompanyName\": \"Adobe Systems Inc\",\n          \"Website\": \"adobe.com\",\n          \"PhoneNumber\": \"867854646545443\",\n          \"Country\": \"JP\",\n          \"PostalCode\": \"95110\",\n          \"State\": \"富山県\",\n          \"City\": \"San Jose\",\n          \"Email\": \"bonepall+jan25_03@adobetest.com\",\n          \"Id\": \"00Q1k0000011gx9EAA\",\n          \"comscore_web_desk_mth_uvs_per_domain\": null,\n          \"profound_local_links_level_2_count\": null,\n          \"profound_max_dbi_density\": null,\n          \"has_prior_oppty_contact\": null,\n          \"days_since_last_lead_acct\": 316,\n          \"rankings_list_fortune_1000\": null,\n          \"builtwith_tealium\": null,\n          \"has_prior_oppty_acct\": 0,\n          \"comscore_web_mobile_pvs_per_uv_per_domain\": null,\n          \"lead_activity_subtype\": \"RFI (Request for Information)\",\n          \"lead_product_outlook_group\": \"AEM Forms\",\n          \"contact_job\": \"Senior Manager/Manager\",\n          \"lead_activity_type_count_contact\": null,\n          \"lead_activity_type_count_acct\": 1,\n          \"lead_geo\": \"Japan\",\n          \"builtwith_amo_comp_count\": null,\n          \"comscore_web_desk_visits_per_uv_per_domain\": null,\n          \"curr_buy_size\": null,\n          \"profound___change\": null,\n          \"comscore_web_total_mth_visits_per_domain\": null,\n          \"lead_solution_count_contact\": null,\n          \"contact_department\": \"Marketing: Automation\",\n          \"adobe_website_amc_only_video_views\": null,\n          \"profound_external_links_homepage_count\": null,\n          \"lead_solution_count_acct\": 1,\n          \"lead_activity_type\": \"Offer\",\n          \"days_since_last_lead_contact\": null,\n          \"comscore_web_desk_mth_pvs_per_domain\": null,\n          \"lead_source_count_contact\": null,\n          \"adobe_website_amo_return_visits\": null,\n          \"adobe_website_aem_downloads\": null,\n          \"comscore_web_desk_mth_visits_per_domain\": null,\n          \"profound_subdomain_count\": null,\n          \"comscore_web_desk_pvs_per_uv_per_domain\": null,\n          \"comscore_web_mobile_visits_per_uv_per_domain\": null,\n          \"lead_source_count_acct\": 0\n        }\n      },\n      \"performEnrichment\": false,\n      \"rule\": \"PMML Japan DigMa Scoring Rule\"\n    }\n  ]\n}";

    private BulkRecordScoreRequest bulkRequest;

    @Mock
    private ScoreRequest request;

    @Mock
    private ScoringArtifacts scoringArtifacts;

    @Mock
    private ModelSummary modelSummary;

    @Mock
    private ModelRetriever modelRetriever;

    @Mock
    protected HttpStopWatch httpStopWatch;

    @Mock
    private RequestInfo requestInfo;

    @Mock
    private Warnings warnings;

    @Autowired
    @InjectMocks
    private ScoreRequestProcessor scoreRequestProcessor;

    @BeforeClass(groups = "functional")
    public void setup() {
        bulkRequest = JsonUtils.deserialize(requestStr, BulkRecordScoreRequest.class);
        log.info("request is " + bulkRequest);
        MockitoAnnotations.initMocks(this);
        space = CustomerSpace.parse("space");
        when(request.getModelId()).thenReturn("modelId");
        when(request.getRule()).thenReturn("");
        when(request.getSource()).thenReturn("");
        when(httpStopWatch.split("requestPreparation")).thenReturn(1L);
        when(httpStopWatch.split("retrieveModelArtifacts")).thenReturn(2L);
        when(httpStopWatch.split("parseRecord")).thenReturn(3L);
        when(httpStopWatch.split("matchRecord")).thenReturn(4L);
        when(httpStopWatch.split("transformRecord")).thenReturn(5L);
        when(httpStopWatch.split("scoreRecord")).thenReturn(6L);
        Mockito.doNothing().when(requestInfo).put(any(String.class), any(String.class));
        when(modelSummary.getStatus()).thenReturn(ModelSummaryStatus.INACTIVE);
        when(modelSummary.getName()).thenReturn("modelName");
        when(modelSummary.getId()).thenReturn("modelId");
        when(scoringArtifacts.getModelSummary()).thenReturn(modelSummary);
        when(modelRetriever.getModelArtifacts(any(CustomerSpace.class), any(String.class)))
                .thenReturn(scoringArtifacts);
    }

    @Test(groups = "functional")
    public void testProcessSingleRecord() {
        boolean thrownException = false;
        try {
            AdditionalScoreConfig additionalScoreConfig = AdditionalScoreConfig.instance() //
                    .setSpace(space) //
                    .setDebug(false) //
                    .setEnrichInternalAttributes(false) //
                    .setPerformFetchOnlyForMatching(false) //
                    .setRequestId("requestId") //
                    .setCalledViaApiConsole(false) //
                    .setEnforceFuzzyMatch(false) //
                    .setSkipDnBCache(false) //
                    .setForceSkipMatching(false);

            scoreRequestProcessor.process(request, additionalScoreConfig);
        } catch (Exception e) {
            thrownException = true;
            Assert.assertTrue(e instanceof LedpException);
            Assert.assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_31114);
        }
        Assert.assertTrue(thrownException, "Should have thrown exception");
    }

    @Test(groups = "functional", dependsOnMethods = { "testProcessSingleRecord" })
    public void testProcessBulkScoreRequest() {
        when(modelRetriever.getModelArtifacts(any(CustomerSpace.class), any(String.class)))
                .thenThrow(new LedpException(LedpCode.LEDP_31026, new String[] { modelId }));
        when(warnings.getWarnings(any(String.class))).thenReturn(Collections.emptyList());
        try {
            AdditionalScoreConfig additionalScoreConfig = AdditionalScoreConfig.instance() //
                    .setSpace(space) //
                    .setDebug(false) //
                    .setEnrichInternalAttributes(false) //
                    .setPerformFetchOnlyForMatching(false) //
                    .setRequestId(requestId) //
                    .setHomogeneous(bulkRequest.isHomogeneous()) //
                    .setForceSkipMatching(false);

            List<RecordScoreResponse> responseList = scoreRequestProcessor.process(bulkRequest, additionalScoreConfig);
            log.info(String.format("ResponseList is %s", responseList.toString()));
            String errorStr = responseList.get(0).getScores().get(0).getError();
            Assert.assertNotNull(errorStr);
            Assert.assertEquals(errorStr, LedpCode.LEDP_31026.toString());
        } catch (Exception ignore) {
            // ignored because this is to testing an exception case
        }
    }

}
