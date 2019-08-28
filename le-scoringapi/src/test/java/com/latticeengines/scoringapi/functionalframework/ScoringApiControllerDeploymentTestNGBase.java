package com.latticeengines.scoringapi.functionalframework;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.oauth2.client.OAuth2RestTemplate;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;

import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.oauth.OAuthUser;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttributesOperationMap;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.scoringapi.BulkRecordScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.DataComposition;
import com.latticeengines.domain.exposed.scoringapi.Field;
import com.latticeengines.domain.exposed.scoringapi.FieldInterpretation;
import com.latticeengines.domain.exposed.scoringapi.FieldInterpretationCollections;
import com.latticeengines.domain.exposed.scoringapi.Fields;
import com.latticeengines.domain.exposed.scoringapi.ModelDetail;
import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.lp.CreateBucketMetadataRequest;
import com.latticeengines.oauth2db.exposed.entitymgr.OAuthUserEntityMgr;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;
import com.latticeengines.proxy.exposed.lp.BucketedScoreProxy;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.oauth2.LatticeOAuth2RestTemplateFactory;
import com.latticeengines.proxy.exposed.pls.PlsInternalProxy;
import com.latticeengines.scoringapi.controller.TestModelArtifactDataComposition;
import com.latticeengines.scoringapi.controller.TestModelConfiguration;
import com.latticeengines.scoringapi.controller.TestRegisterModels;
import com.latticeengines.scoringapi.exposed.model.ModelJsonTypeHandler;
import com.latticeengines.scoringapi.exposed.model.impl.ModelRetrieverImpl;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;
import com.latticeengines.testframework.service.impl.GlobalAuthDeploymentTestBed;

@Listeners({GlobalAuthCleanupTestListener.class})
public class ScoringApiControllerDeploymentTestNGBase extends ScoringApiFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ScoringApiControllerDeploymentTestNGBase.class);

    protected static final String TEST_MODEL_FOLDERNAME = "3MulesoftAllRows20160314_112802";
    protected static final String MODEL_NAME = TEST_MODEL_FOLDERNAME;
    protected static final String LOCAL_MODEL_PATH = "com/latticeengines/scoringapi/model/" + TEST_MODEL_FOLDERNAME
            + "/";
    protected static final String APPLICATION_ID = "application_1457046993615_3821";
    protected static final String PARSED_APPLICATION_ID = "1457046993615_3821";
    protected static final String MODEL_VERSION = "8ba99b36-c222-4f93-ab8a-6dcc11ce45e9";
    protected static final String EVENT_TABLE = TEST_MODEL_FOLDERNAME;
    protected static final String SOURCE_INTERPRETATION = "SalesforceLead";
    protected static final String MODELSUMMARYJSON_LOCALPATH = LOCAL_MODEL_PATH + ModelRetrieverImpl.MODEL_JSON;
    protected static final String CLIENT_ID_LP = "lp";
    protected static final String CLIENT_ID_PLAYMAKER = "playmaker";

    private static final String DUMMY_APP_ID = "DUMMY_APP";
    public CustomerSpace customerSpace;
    protected String MODEL_ID;

    @Value("${common.test.scoringapi.url}")
    protected String apiHostPort;

    @Value("${common.test.oauth.url}")
    protected String authHostPort;

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    protected LatticeOAuth2RestTemplateFactory latticeOAuth2RestTemplateFactory;

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    @Inject
    protected MetadataProxy metadataProxy;

    @Autowired
    @Qualifier(value = "deploymentTestBed")
    protected GlobalAuthDeploymentTestBed deploymentTestBed;

    @Inject
    private BucketedScoreProxy bucketedScoreProxy;

    @Inject
    protected ModelSummaryProxy modelSummaryProxy;

    @Inject
    protected PlsInternalProxy plsInternalProxy;

    protected OAuthUserEntityMgr userEntityMgr;

    protected DataComposition eventTableDataComposition;

    protected DataComposition dataScienceDataComposition;

    protected OAuthUser oAuthUser;

    protected OAuth2RestTemplate oAuth2RestTemplate = null;

    protected Tenant tenant;

    protected TestModelSummaryParser testModelSummaryParser;

    protected List<String> selectedAttributes;

    @BeforeClass(groups = "deployment")
    public void beforeClass() throws IOException {
        testModelSummaryParser = new TestModelSummaryParser();
        MODEL_ID = generateRandomModelId();
        tenant = setupTenantAndModelSummary(true);
        setupHdfsArtifacts(tenant);

        if (shouldInit()) {
            userEntityMgr = applicationContext.getBean(OAuthUserEntityMgr.class);
            oAuthUser = getOAuthUser(tenant.getId());

            oAuth2RestTemplate = createOAuth2RestTemplate(CLIENT_ID_LP);
        }

        if (shouldSelectAttributeBeforeTest()) {
            saveAttributeSelectionBeforeTest(customerSpace);
        }
    }

    protected OAuth2RestTemplate createOAuth2RestTemplate(String clientId) {
        OAuth2RestTemplate oAuth2RestTemplate = null;
        if (shouldUseAppId()) {
            log.info(String.format(
                    "Requesting access token for app id = %s, user id = %s, password = %s, client id = %s",
                    getAppIdForOauth2(), oAuthUser.getUserId(), oAuthUser.getPassword(), clientId));
            oAuth2RestTemplate = latticeOAuth2RestTemplateFactory.getOAuth2RestTemplate(oAuthUser, clientId,
                    getAppIdForOauth2(), authHostPort);
        } else {
            oAuth2RestTemplate = OAuth2Utils.getOauthTemplate(authHostPort, oAuthUser.getUserId(),
                    oAuthUser.getPassword(), clientId);
        }
        OAuth2AccessToken accessToken = OAuth2Utils.getAccessToken(oAuth2RestTemplate);
        log.info(String.format("Oauth access token = %s", accessToken.getValue()));

        return oAuth2RestTemplate;
    }

    protected boolean shouldInit() {
        return true;
    }

    @AfterClass(groups = "deployment")
    public void afterClass() {
        if (userEntityMgr != null) {
            userEntityMgr.delete(oAuthUser.getUserId());
        }
        deploymentTestBed.deleteTenant(tenant);
    }

    protected OAuthUser getOAuthUser(String userId) {
        OAuthUser user = null;
        try {
            user = userEntityMgr.get(userId);
        } catch (Exception ex) {
            log.info("OAuth user does not exist! userId=" + userId);
        }
        if (user == null) {
            user = new OAuthUser();
            user.setUserId(userId);
            setPassword(user, userId);
            userEntityMgr.create(user);
        } else {
            setPassword(user, userId);
            user.setPasswordExpired(false);
            userEntityMgr.update(user);
        }

        return user;
    }

    protected boolean shouldSelectAttributeBeforeTest() {
        return true;
    }

    protected void saveAttributeSelectionBeforeTest(CustomerSpace customerSpace) {
        LeadEnrichmentAttributesOperationMap selectedAttributeMap = checkSelection(customerSpace);
        plsInternalProxy.saveLeadEnrichmentAttributes(customerSpace, selectedAttributeMap);
        Assert.assertNotNull(selectedAttributes);
        Assert.assertEquals(selectedAttributes.size(), 6);
    }

    private LeadEnrichmentAttributesOperationMap checkSelection(CustomerSpace customerSpace) {
        List<LeadEnrichmentAttribute> enrichmentAttributeList = plsInternalProxy
                .getLeadEnrichmentAttributes(customerSpace, null, null, false, false);
        LeadEnrichmentAttributesOperationMap selectedAttributeMap = new LeadEnrichmentAttributesOperationMap();
        selectedAttributes = new ArrayList<>();
        selectedAttributeMap.setSelectedAttributes(selectedAttributes);
        List<String> deselectedAttributes = new ArrayList<>();
        selectedAttributeMap.setDeselectedAttributes(deselectedAttributes);
        int premiumSelectCount = 2;
        int selectCount = 4;

        for (LeadEnrichmentAttribute attr : enrichmentAttributeList) {
            if (attr.getIsPremium()) {
                if (premiumSelectCount > 0) {
                    premiumSelectCount--;
                    selectedAttributes.add(attr.getFieldName());
                }
            } else {
                if (selectCount > 0) {
                    selectCount--;
                    selectedAttributes.add(attr.getFieldName());
                }
            }
        }

        return selectedAttributeMap;
    }

    private void setPassword(OAuthUser user, String userId) {
        user.setPassword(OAuth2Utils.generatePassword());
        user.setPasswordExpiration(userEntityMgr.getPasswordExpiration(userId));
    }

    protected void createModel(String modelName, String modelId, CustomerSpace customerSpace, Tenant tenant)
            throws IOException {
        ModelSummary retrievedSummary = modelSummaryProxy.getModelSummaryFromModelId(tenant.getId(), modelId);
        if (retrievedSummary != null) {
            modelSummaryProxy.deleteByModelId(customerSpace.toString(), modelId);
        }
        Map<TestModelConfiguration, TestModelArtifactDataComposition> models = new HashMap<>();
        TestRegisterModels modelCreator = new TestRegisterModels();
        long timestamp = System.currentTimeMillis();
        String hdfsSubPathForModel = "Event";

        hdfsSubPathForModel = "Random" + 0;

        String applicationId = "application_" + 0 + "1457046993615_3823_" + timestamp;
        String modelVersion = "ba99b36-c222-4f93" + 0 + "-ab8a-6dcc11ce45e9-" + timestamp;
        TestModelConfiguration modelConfiguration = null;
        TestModelArtifactDataComposition modelArtifactDataComposition = null;
        modelConfiguration = new TestModelConfiguration(modelName, modelId, applicationId, modelVersion);
        modelArtifactDataComposition = modelCreator.createModels(yarnConfiguration, bucketedScoreProxy,
                columnMetadataProxy, (tenant != null ? tenant : this.tenant), modelConfiguration,
                (customerSpace != null ? customerSpace : this.customerSpace), metadataProxy,
                getTestModelSummaryParser(), hdfsSubPathForModel, modelSummaryProxy);

        models.put(modelConfiguration, modelArtifactDataComposition);
        System.out.println("Registered model: " + modelId);
    }

    protected Tenant setupTenantAndModelSummary(boolean includeApplicationId) throws IOException {

        Tenant tenant = deploymentTestBed.bootstrapForProduct(LatticeProduct.LPA3);
        deploymentTestBed.switchToSuperAdmin();
        customerSpace = CustomerSpace.parse(tenant.getId());
        createModel(MODEL_NAME, MODEL_ID, customerSpace, tenant);

        List<BucketMetadata> bucketMetadataList = ScoringApiTestUtils.generateDefaultBucketMetadataList();
        CreateBucketMetadataRequest request = new CreateBucketMetadataRequest();
        request.setBucketMetadataList(bucketMetadataList);
        request.setModelGuid(MODEL_ID);
        bucketedScoreProxy.createABCDBuckets(customerSpace.toString(), request);
        return tenant;
    }

    private void setupHdfsArtifacts(Tenant tenant) throws IOException {
        String tenantId = tenant.getId();
        String artifactTableDir = String.format(ModelRetrieverImpl.HDFS_SCORE_ARTIFACT_EVENTTABLE_DIR, tenantId,
                EVENT_TABLE);
        artifactTableDir = artifactTableDir.replaceAll("\\*", "Event");
        String artifactBaseDir = String.format(ModelRetrieverImpl.HDFS_SCORE_ARTIFACT_BASE_DIR, tenantId, EVENT_TABLE,
                MODEL_VERSION, PARSED_APPLICATION_ID);
        String enhancementsDir = artifactBaseDir + ModelJsonTypeHandler.HDFS_ENHANCEMENTS_DIR;

        InputStream eventTableDataCompositionUrl = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream(LOCAL_MODEL_PATH + "eventtable-" + ModelJsonTypeHandler.DATA_COMPOSITION_FILENAME);
        InputStream modelJsonUrl = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream(MODELSUMMARYJSON_LOCALPATH);
        InputStream rfpmmlUrl = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream(LOCAL_MODEL_PATH + ModelJsonTypeHandler.PMML_FILENAME);
        InputStream dataScienceDataCompositionUrl = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream(
                        LOCAL_MODEL_PATH + "datascience-" + ModelJsonTypeHandler.DATA_COMPOSITION_FILENAME);
        InputStream scoreDerivationUrl = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream(LOCAL_MODEL_PATH + ModelJsonTypeHandler.SCORE_DERIVATION_FILENAME);

        HdfsUtils.rmdir(yarnConfiguration, artifactTableDir);
        HdfsUtils.rmdir(yarnConfiguration, artifactBaseDir);
        HdfsUtils.rmdir(yarnConfiguration, enhancementsDir);

        HdfsUtils.mkdir(yarnConfiguration, artifactTableDir);
        HdfsUtils.mkdir(yarnConfiguration, artifactBaseDir);
        HdfsUtils.mkdir(yarnConfiguration, enhancementsDir);
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, eventTableDataCompositionUrl,
                artifactTableDir + ModelJsonTypeHandler.DATA_COMPOSITION_FILENAME);
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, modelJsonUrl,
                artifactBaseDir + TEST_MODEL_FOLDERNAME + "_model.json");
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, rfpmmlUrl,
                artifactBaseDir + ModelJsonTypeHandler.PMML_FILENAME);
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, dataScienceDataCompositionUrl,
                enhancementsDir + ModelJsonTypeHandler.DATA_COMPOSITION_FILENAME);
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, scoreDerivationUrl,
                enhancementsDir + ModelJsonTypeHandler.SCORE_DERIVATION_FILENAME);

        eventTableDataCompositionUrl = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream(LOCAL_MODEL_PATH + "eventtable-" + ModelJsonTypeHandler.DATA_COMPOSITION_FILENAME);
        String eventTableDataCompositionContents = IOUtils.toString(eventTableDataCompositionUrl,
                Charset.defaultCharset());
        eventTableDataComposition = JsonUtils.deserialize(eventTableDataCompositionContents, DataComposition.class);

        dataScienceDataCompositionUrl = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream(
                        LOCAL_MODEL_PATH + "datascience-" + ModelJsonTypeHandler.DATA_COMPOSITION_FILENAME);
        String dataScienceDataCompositionContents = IOUtils.toString(dataScienceDataCompositionUrl,
                Charset.defaultCharset());
        dataScienceDataComposition = JsonUtils.deserialize(dataScienceDataCompositionContents, DataComposition.class);
    }

    protected ScoreRequest getScoreRequest() throws IOException {
        return getScoreRequest(false);
    }

    protected ScoreRequest getScoreRequest(boolean isPmmlModel) throws IOException {
        String scoreRecordContents = null;
        if (isPmmlModel) {
            scoreRecordContents = pmmlImputJson();
        } else {
            InputStream scoreRequestUrl = Thread.currentThread().getContextClassLoader() //
                    .getResourceAsStream(isPmmlModel ? null : LOCAL_MODEL_PATH + "score_request.json");
            scoreRecordContents = IOUtils.toString(scoreRequestUrl, Charset.defaultCharset());
        }
        ScoreRequest scoreRequest = JsonUtils.deserialize(scoreRecordContents, ScoreRequest.class);
        scoreRequest.setModelId(MODEL_ID);
        return scoreRequest;
    }

    protected BulkRecordScoreRequest getBulkScoreRequestForScoreCorrectness() throws IOException {
        BulkRecordScoreRequest scoreRequest = JsonUtils.deserialize(bulkRecordInputForErrorCorrectnessTest(),
                BulkRecordScoreRequest.class);
        return scoreRequest;
    }

    protected List<ScoreRequest> getScoreRequestsForScoreCorrectness() throws IOException {
        List<ScoreRequest> scoreRequests = new ArrayList<>();
        ScoreRequest scoreRequest = JsonUtils.deserialize(singleRecordInput1ForErrorCorrectnessTest(),
                ScoreRequest.class);
        scoreRequests.add(scoreRequest);
        scoreRequest = JsonUtils.deserialize(singleRecordInput2ForErrorCorrectnessTest(), ScoreRequest.class);
        scoreRequests.add(scoreRequest);
        scoreRequest = JsonUtils.deserialize(singleRecordInput3ForErrorCorrectnessTest(), ScoreRequest.class);
        scoreRequests.add(scoreRequest);
        scoreRequest = JsonUtils.deserialize(singleRecordInput4ForErrorCorrectnessTest(), ScoreRequest.class);
        scoreRequests.add(scoreRequest);
        return scoreRequests;
    }

    protected List<Integer> getExpectedScoresForScoreCorrectness() {
        List<Integer> expectedScores = new ArrayList<>();
        int[] expected = {90, 71, 91, 88};
        // (YSong) When cutting M25 release RC, 3rd expected changed from 89 to 91.
        // The reason for the score change is still unknown, might be DC 2.0.16
        // release.
        int[] newExpected = {96, 89, 96, 94};
        // TODO - data model score not match with csv records
        for (int score : newExpected) {
            expectedScores.add(score);
        }
        return expectedScores;
    }

    protected void checkModelDetails(List<ModelDetail> models, String modelNamePrefix, String fieldDisplayNamePrefix)
            throws ParseException {
        Assert.assertNotNull(models);
        Assert.assertTrue(models.size() >= 1);
        Assert.assertTrue(models.size() <= 50);
        for (ModelDetail model : models) {
            Assert.assertNotNull(model.getFields());
            Assert.assertNotNull(model.getFields().getFields());
            Assert.assertTrue(model.getFields().getFields().size() > 1);
            Assert.assertNotNull(model.getModel());
            Assert.assertNotNull(model.getModel().getModelId());
            Assert.assertNotNull(model.getModel().getName());
            Assert.assertNotNull(model.getStatus());
            Assert.assertNotNull(model.getLastModifiedTimestamp());

            Assert.assertNotNull(DateTimeUtils.convertToDateUTCISO8601(model.getLastModifiedTimestamp()));

            checkFields(model.getModel().getName(), model.getFields(), modelNamePrefix, fieldDisplayNamePrefix);
        }
    }

    protected void checkFields(String modelName, Fields fields, String modelNamePrefix, String fieldDisplayNamePrefix) {
        for (Field field : fields.getFields()) {
            Assert.assertNotNull(field.getFieldName());
            Assert.assertNotNull(field.getFieldType());
            if (modelName.startsWith(modelNamePrefix)) {
                String displayName = field.getDisplayName();
                Assert.assertNotNull(displayName);
            }
            try {
                FieldInterpretation fi = FieldInterpretation.valueOf(field.getFieldName());
                Assert.assertEquals(field.isPrimaryField(),
                        FieldInterpretationCollections.PrimaryMatchingFields.contains(fi));
            } catch (Exception e) {
                // Ignore. As it is not a Mapped to FieldInterpretation
            }
        }
    }

    protected boolean shouldUseAppId() {
        return false;
    }

    protected String getAppIdForOauth2() {
        return DUMMY_APP_ID;
    }

    public TestModelSummaryParser getTestModelSummaryParser() {
        return testModelSummaryParser;
    }

    private String singleRecordInput1ForErrorCorrectnessTest() {
        return "    {\"modelId\":\"" + MODEL_ID
                + "\",\"source\":\"APIConsole\",\"performEnrichment\":false,\"rule\":\"manual\"," + //
                "    \"record\":{" + //
                "    \"Id\":\"1726380\",\"Email\":\"vasanthi.sontha@j2.com\",\"CompanyName\":\"J2 Global\",\"City\":\"Hollywood\",\"State\":\"CA\",\"Country\":\"United States\",\"PostalCode\":\"90028\""
                + //
                "    }" + //
                "    }";
    }

    private String singleRecordInput2ForErrorCorrectnessTest() {
        return "    {\"modelId\":\"" + MODEL_ID
                + "\",\"source\":\"APIConsole\",\"performEnrichment\":false,\"rule\":\"manual\"," + //
                "    \"record\":{" + //
                "    \"Id\":\"1723589\",\"Email\":\"mdixon@cbiz.com\",\"CompanyName\":\"Missouri\",\"City\":\"FIndependence\",\"State\":\"MO\",\"Country\":\"United States\",\"PostalCode\":\"44131-6951\"}"
                + //
                "    }";
    }

    private String singleRecordInput3ForErrorCorrectnessTest() {
        return "    {\"modelId\":\"" + MODEL_ID
                + "\",\"source\":\"APIConsole\",\"performEnrichment\":false,\"rule\":\"manual\"," + //
                "    \"record\":{" + //
                "    \"Id\":\"1723653\",\"Email\":\"andy.chu@mineralstech.com\",\"CompanyName\":\"Minerals Tech\",\"City\":\"New York\",\"State\":\"NY\",\"Country\":\"United States\",\"PostalCode\":\"10017-6729\""
                + //
                "    }" + //
                "    }";
    }

    private String singleRecordInput4ForErrorCorrectnessTest() {
        return "    {\"modelId\":\"" + MODEL_ID
                + "\",\"source\":\"APIConsole\",\"performEnrichment\":false,\"rule\":\"manual\"," + //
                "    \"record\":{" + //
                "    \"Id\":\"1723736\",\"Email\":\"ybelenky@miraclesoft.com\",\"CompanyName\":\"Miracle Software Systems, Inc.\",\"City\":\"Novi\",\"State\":\"MI\",\"Country\":\"United States\",\"PostalCode\":\"48374\"}"
                + //
                "    }";
    }

    private String bulkRecordInputForErrorCorrectnessTest() {
        return "{\n" +
                "  \"source\": \"Dummy Source\",\n" +
                "  \"records\": [\n" +
                "    {\n" +
                "      \"recordId\": \"2\",\n" +
                "      \"idType\": \"LATTICE\",\n" +
                "      \"modelAttributeValuesMap\": {\n" +
                "        \"" + MODEL_ID + "\": {\n" +
                "          \"Email\": \"vasanthi.sontha@j2.com\",\n" +
                "          \"Id\": \"1726380\",\n" +
                "          \"CompanyName\": \"J2 Global\",\n" +
                "          \"City\": \"Hollywood\",\n" +
                "          \"State\": \"CA\",\n" +
                "          \"Country\": \"United States\",\n" +
                "          \"PostalCode\": \"90028\"\n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"recordId\": \"4\",\n" +
                "      \"idType\": \"LATTICE\",\n" +
                "      \"modelAttributeValuesMap\": {\n" +
                "        \"" + MODEL_ID + "\": {\n" +
                "          \"Email\": \"mdixon@cbiz.com\",\n" +
                "          \"Id\": \"1723589\",\n" +
                "          \"CompanyName\": \"Missouri\",\n" +
                "          \"City\": \"FIndependence\",\n" +
                "          \"State\": \"MO\",\n" +
                "          \"Country\": \"United States\",\n" +
                "          \"PostalCode\": \"44131-6951\"\n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"recordId\": \"3\",\n" +
                "      \"idType\": \"LATTICE\",\n" +
                "      \"modelAttributeValuesMap\": {\n" +
                "        \"" + MODEL_ID + "\": {\n" +
                "          \"Email\": \"andy.chu@mineralstech.com\",\n" +
                "          \"Id\": \"1723653\",\n" +
                "          \"CompanyName\": \"Minerals Tech\",\n" +
                "          \"City\": \"New York\",\n" +
                "          \"State\": \"NY\",\n" +
                "          \"Country\": \"United States\",\n" +
                "          \"PostalCode\": \"10017-6729\"\n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"recordId\": \"36c5c666-1eb4-4221-b5d3-93b23be72a6e\",\n" +
                "      \"idType\": \"LATTICE\",\n" +
                "      \"modelAttributeValuesMap\": {\n" +
                "        \"" + MODEL_ID + "\": {\n" +
                "          \"Email\": \"ybelenky@miraclesoft.com\",\n" +
                "          \"Id\": \"1723736\",\n" +
                "          \"CompanyName\": \"Miracle Software Systems, Inc.\",\n" +
                "          \"City\": \"Novi\",\n" +
                "          \"State\": \"MI\",\n" +
                "          \"Country\": \"United States\",\n" +
                "          \"PostalCode\": \"48374\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
                "}";
    }

    private String pmmlImputJson() {
        return "{" + //
                "    \"modelId\": \"" + MODEL_ID + "\"," + //
                "    \"record\": {" + //
                "    \"BusinessECommerceSite\":\"1\"," + //
                "    \"BusinessVCFunded\":\"1\"," + //
                "    \"JobsTrendString\":\"1\"," + //
                "    \"PD_DA_LastSocialActivity_Units\":\"1\"," + //
                "    \"BankruptcyFiled\":\"1\"," + //
                "    \"BusinessAnnualSalesAbs\":\"1\"," + //
                "    \"BusinessEstablishedYear\":\"1\"," + //
                "    \"BusinessEstimatedAnnualSales_k\":\"1\"," + //
                "    \"BusinessEstimatedEmployees\":\"1\"," + //
                "    \"BusinessFirmographicsParentEmployees\":\"1\"," + //
                "    \"BusinessFirmographicsParentRevenue\":\"1\"," + //
                "    \"BusinessRetirementParticipants\":\"1\"," + //
                "    \"BusinessSocialPresence\":\"1\"," + //
                "    \"BusinessUrlNumPages\":\"1\"," + //
                "    \"DerogatoryIndicator\":\"1\"," + //
                "    \"ExperianCreditRating\":\"1\"," + //
                "    \"FundingAgency\":\"1\"," + //
                "    \"FundingAmount\":\"1\"," + //
                "    \"FundingAwardAmount\":\"1\"," + //
                "    \"FundingFinanceRound\":\"1\"," + //
                "    \"FundingReceived\":\"1\"," + //
                "    \"FundingStage\":\"1\"," + //
                "    \"Intelliscore\":\"1\"," + //
                "    \"JobsRecentJobs\":\"1\"," + //
                "    \"ModelAction\":\"1\"," + //
                "    \"PercentileModel\":\"1\"," + //
                "    \"UCCFilings\":\"1\"," + //
                "    \"UCCFilingsPresent\":\"1\"," + //
                "    \"Years_in_Business_Code\":\"1\"," + //
                "    \"Non_Profit_Indicator\":\"1\"," + //
                "    \"PD_DA_AwardCategory\":\"1\"," + //
                "    \"PD_DA_MonthsPatentGranted\":\"1\"," + //
                "    \"PD_DA_MonthsSinceFundAwardDate\":\"1\"," + //
                "    \"PD_DA_PrimarySIC1\":\"1\"," + //
                "    \"AnnualRevenue\":\"1\"," + //
                "    \"NumberOfEmployees\":\"1\"," + //
                "    \"Alexa_MonthsSinceOnline\":\"1\"," + //
                "    \"Alexa_Rank\":\"1\"," + //
                "    \"Alexa_ReachPerMillion\":\"1\"," + //
                "    \"Alexa_ViewsPerMillion\":\"1\"," + //
                "    \"Alexa_ViewsPerUser\":\"1\"," + //
                "    \"BW_TechTags_Cnt\":\"1\"," + //
                "    \"BW_TotalTech_Cnt\":\"1\"," + //
                "    \"BW_ads\":\"1\"," + //
                "    \"BW_analytics\":\"1\"," + //
                "    \"BW_cdn\":\"1\"," + //
                "    \"BW_cdns\":\"1\"," + //
                "    \"BW_cms\":\"1\"," + //
                "    \"BW_docinfo\":\"1\"," + //
                "    \"BW_encoding\":\"1\"," + //
                "    \"BW_feeds\":\"1\"," + //
                "    \"BW_framework\":\"1\"," + //
                "    \"BW_hosting\":\"1\"," + //
                "    \"BW_javascript\":\"1\"," + //
                "    \"BW_mapping\":\"1\"," + //
                "    \"BW_media\":\"1\"," + //
                "    \"BW_mx\":\"1\"," + //
                "    \"BW_ns\":\"1\"," + //
                "    \"BW_parked\":\"1\"," + //
                "    \"BW_payment\":\"1\"," + //
                "    \"BW_seo_headers\":\"1\"," + //
                "    \"BW_seo_meta\":\"1\"," + //
                "    \"BW_seo_title\":\"1\"," + //
                "    \"BW_Server\":\"1\"," + //
                "    \"BW_shop\":\"1\"," + //
                "    \"BW_ssl\":\"1\"," + //
                "    \"BW_Web_Master\":\"1\"," + //
                "    \"BW_Web_Server\":\"1\"," + //
                "    \"BW_widgets\":\"1\"," + //
                "    \"Activity_ClickLink_cnt\":\"1\"," + //
                "    \"Activity_VisitWeb_cnt\":\"1\"," + //
                "    \"Activity_InterestingMoment_cnt\":\"1\"," + //
                "    \"Activity_OpenEmail_cnt\":\"1\"," + //
                "    \"Activity_EmailBncedSft_cnt\":\"1\"," + //
                "    \"Activity_FillOutForm_cnt\":\"1\"," + //
                "    \"Activity_UnsubscrbEmail_cnt\":\"1\"," + //
                "    \"Activity_ClickEmail_cnt\":\"1\"," + //
                "    \"CloudTechnologies_CloudService_One\":\"1\"," + //
                "    \"CloudTechnologies_CloudService_Two\":\"1\"," + //
                "    \"CloudTechnologies_CommTech_One\":\"1\"," + //
                "    \"CloudTechnologies_CommTech_Two\":\"1\"," + //
                "    \"CloudTechnologies_CRM_One\":\"1\"," + //
                "    \"CloudTechnologies_CRM_Two\":\"1\"," + //
                "    \"CloudTechnologies_EnterpriseApplications_One\":\"1\"," + //
                "    \"CloudTechnologies_EnterpriseApplications_Two\":\"1\"," + //
                "    \"CloudTechnologies_EnterpriseContent_One\":\"1\"," + //
                "    \"CloudTechnologies_EnterpriseContent_Two\":\"1\"," + //
                "    \"CloudTechnologies_HardwareBasic_One\":\"1\"," + //
                "    \"CloudTechnologies_HardwareBasic_Two\":\"1\"," + //
                "    \"CloudTechnologies_ITGovernance_One\":\"1\"," + //
                "    \"CloudTechnologies_ITGovernance_Two\":\"1\"," + //
                "    \"CloudTechnologies_MarketingPerfMgmt_One\":\"1\"," + //
                "    \"CloudTechnologies_MarketingPerfMgmt_Two\":\"1\"," + //
                "    \"CloudTechnologies_ProductivitySltns_One\":\"1\"," + //
                "    \"CloudTechnologies_ProductivitySltns_Two\":\"1\"," + //
                "    \"CloudTechnologies_ProjectMgnt_One\":\"1\"," + //
                "    \"CloudTechnologies_ProjectMgnt_Two\":\"1\"," + //
                "    \"CloudTechnologies_SoftwareBasic_One\":\"1\"," + //
                "    \"CloudTechnologies_SoftwareBasic_Two\":\"1\"," + //
                "    \"PD_DA_LastSocialActivity_Units_days\":\"1\"," + //
                "    \"JobsTrendString_Aggressively_Hiring\":\"1\"," + //
                "    \"JobsTrendString_Moderately_Hiring\":\"1\"," + //
                "    \"BusinessVCFunded_True\":\"1\"," + //
                "    \"JobsTrendString_Significantly_Hiring\":\"1\"," + //
                "    \"BusinessECommerceSite_True\":\"1\"," + //
                "    \"BusinessECommerceSite__1\":\"1\"," + //
                "    \"PD_DA_LastSocialActivity_Units_minutes\":\"1\"," + //
                "    \"PD_DA_LastSocialActivity_Units_hours\":\"1\"," + //
                "    \"PD_DA_LastSocialActivity_Units_seconds\":\"1\"," + //
                "    \"BusinessVCFunded_False\":\"1\"," + //
                "    \"BusinessECommerceSite_False\":\"1\"," + //
                "    \"BusinessVCFunded__1\":\"1\"," + //
                "    \"PD_DA_JobTitle\":\"1\"" + //
                "    }" + //
                "}";
    }
}
