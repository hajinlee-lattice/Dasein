package com.latticeengines.scoringapi.functionalframework;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.oauth2.client.OAuth2RestTemplate;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.oauth.OAuthUser;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttributesOperationMap;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.scoringapi.BulkRecordScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.DataComposition;
import com.latticeengines.domain.exposed.scoringapi.Field;
import com.latticeengines.domain.exposed.scoringapi.FieldInterpretation;
import com.latticeengines.domain.exposed.scoringapi.FieldInterpretationCollections;
import com.latticeengines.domain.exposed.scoringapi.Fields;
import com.latticeengines.domain.exposed.scoringapi.ModelDetail;
import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.oauth2db.exposed.entitymgr.OAuthUserEntityMgr;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;
import com.latticeengines.proxy.exposed.oauth2.LatticeOAuth2RestTemplateFactory;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.scoringapi.exposed.model.ModelJsonTypeHandler;
import com.latticeengines.scoringapi.exposed.model.impl.ModelRetrieverImpl;
import com.latticeengines.testframework.domain.pls.ModelSummaryUtils;

public class ScoringApiControllerDeploymentTestNGBase extends ScoringApiFunctionalTestNGBase {

    protected static final String TEST_MODEL_FOLDERNAME = "3MulesoftAllRows20160314_112802";

    protected static final String MODEL_ID = "ms__" + TEST_MODEL_FOLDERNAME + "_";
    protected static final String MODEL_NAME = TEST_MODEL_FOLDERNAME;
    protected static final String LOCAL_MODEL_PATH = "com/latticeengines/scoringapi/model/" + TEST_MODEL_FOLDERNAME
            + "/";
    protected static final String TENANT_ID = "ScoringApiTestTenant.ScoringApiTestTenant.Production";
    protected static final String APPLICATION_ID = "application_1457046993615_3821";
    protected static final String PARSED_APPLICATION_ID = "1457046993615_3821";
    protected static final String MODEL_VERSION = "8ba99b36-c222-4f93-ab8a-6dcc11ce45e9";
    protected static final String EVENT_TABLE = TEST_MODEL_FOLDERNAME;
    protected static final String SOURCE_INTERPRETATION = "SalesforceLead";
    public static final CustomerSpace customerSpace = CustomerSpace.parse(TENANT_ID);
    protected static final String MODELSUMMARYJSON_LOCALPATH = LOCAL_MODEL_PATH + ModelRetrieverImpl.MODEL_JSON;
    private static final Logger log = LoggerFactory.getLogger(ScoringApiControllerDeploymentTestNGBase.class);

    private static final String CLIENT_ID_LP = "lp";
    private static final String DUMMY_APP_ID = "DUMMY_APP";

    @Value("${common.test.scoringapi.url}")
    protected String apiHostPort;

    @Value("${common.test.oauth.url}")
    protected String authHostPort;

    @Value("${common.test.pls.url}")
    protected String plsApiHostPort;

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    protected LatticeOAuth2RestTemplateFactory latticeOAuth2RestTemplateFactory;

    protected OAuthUserEntityMgr userEntityMgr;

    protected InternalResourceRestApiProxy plsRest;

    protected DataComposition eventTableDataComposition;

    protected DataComposition dataScienceDataComposition;

    protected OAuthUser oAuthUser;

    protected OAuth2RestTemplate oAuth2RestTemplate = null;

    protected Tenant tenant;

    protected InternalResourceRestApiProxy internalResourceRestApiProxy;

    protected TestModelSummaryParser testModelSummaryParser;

    protected List<String> selectedAttributes;

    @BeforeClass(groups = "deployment")
    public void beforeClass() throws IOException {
        testModelSummaryParser = new TestModelSummaryParser();

        if (shouldInit()) {
            userEntityMgr = applicationContext.getBean(OAuthUserEntityMgr.class);
            oAuthUser = getOAuthUser(TENANT_ID);

            if (shouldUseAppId()) {
                System.out.println("Requesting access token for appi id: " + getAppIdForOauth2());
                oAuth2RestTemplate = latticeOAuth2RestTemplateFactory.getOAuth2RestTemplate(oAuthUser, CLIENT_ID_LP,
                        getAppIdForOauth2(), authHostPort);
            } else {
                oAuth2RestTemplate = OAuth2Utils.getOauthTemplate(authHostPort, oAuthUser.getUserId(),
                        oAuthUser.getPassword(), CLIENT_ID_LP);
            }
            OAuth2AccessToken accessToken = OAuth2Utils.getAccessToken(oAuth2RestTemplate);
            log.info(accessToken.getValue());

            System.out.println(accessToken.getValue());
        }

        plsRest = new InternalResourceRestApiProxy(plsApiHostPort);
        tenant = setupTenantAndModelSummary(true);
        setupHdfsArtifacts(tenant);

        if (shouldSelectAttributeBeforeTest()) {
            internalResourceRestApiProxy = new InternalResourceRestApiProxy(plsApiHostPort);
            saveAttributeSelectionBeforeTest(customerSpace);
        }
    }

    protected boolean shouldInit() {
        return true;
    }

    @AfterClass(groups = "deployment")
    public void afterClass() {
        if (userEntityMgr != null) {
            userEntityMgr.delete(oAuthUser.getUserId());
        }
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
        internalResourceRestApiProxy.saveLeadEnrichmentAttributes(customerSpace, selectedAttributeMap);
        Assert.assertNotNull(selectedAttributes);
        Assert.assertEquals(selectedAttributes.size(), 6);
    }

    private LeadEnrichmentAttributesOperationMap checkSelection(CustomerSpace customerSpace) {
        List<LeadEnrichmentAttribute> enrichmentAttributeList = internalResourceRestApiProxy
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

    protected Tenant setupTenantAndModelSummary(boolean includeApplicationId) throws IOException {
        String tenantId = TENANT_ID;
        Tenant tenant = new Tenant();
        tenant.setId(tenantId);
        tenant.setName(tenantId);
        plsRest.deleteTenant(customerSpace);
        plsRest.createTenant(tenant);

        ModelSummary modelSummary = ModelSummaryUtils.generateModelSummary(tenant, MODELSUMMARYJSON_LOCALPATH);
        if (includeApplicationId) {
            modelSummary.setApplicationId(APPLICATION_ID);
        }
        modelSummary.setEventTableName(EVENT_TABLE);
        modelSummary.setId(MODEL_ID);
        modelSummary.setDisplayName(MODEL_NAME);
        modelSummary.setLookupId(String.format("%s|%s|%s", TENANT_ID, EVENT_TABLE, MODEL_VERSION));
        modelSummary.setSourceSchemaInterpretation(SOURCE_INTERPRETATION);
        modelSummary.setStatus(ModelSummaryStatus.ACTIVE);
        modelSummary.setModelType("DUMMY_MODEL_TYPE");
        testModelSummaryParser.setPredictors(modelSummary, MODELSUMMARYJSON_LOCALPATH);

        String modelId = modelSummary.getId();
        ModelSummary retrievedSummary = plsRest.getModelSummaryFromModelId(modelId, customerSpace);
        if (retrievedSummary != null) {
            plsRest.deleteModelSummary(modelId, customerSpace);
        }
        plsRest.createModelSummary(modelSummary, customerSpace);

        List<BucketMetadata> bucketMetadataList = ScoringApiTestUtils.generateDefaultBucketMetadataList();
        plsRest.createABCDBuckets(modelId, customerSpace, bucketMetadataList);

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
                .getResourceAsStream(LOCAL_MODEL_PATH + "datascience-" + ModelJsonTypeHandler.DATA_COMPOSITION_FILENAME);
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
                .getResourceAsStream(LOCAL_MODEL_PATH + "datascience-" + ModelJsonTypeHandler.DATA_COMPOSITION_FILENAME);
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
            scoreRecordContents = pmmlImputJson;
        } else {
            InputStream scoreRequestUrl = Thread.currentThread().getContextClassLoader() //
                    .getResourceAsStream(isPmmlModel ? null : LOCAL_MODEL_PATH + "score_request.json");
            scoreRecordContents = IOUtils.toString(scoreRequestUrl, Charset.defaultCharset());
        }
        ScoreRequest scoreRequest = JsonUtils.deserialize(scoreRecordContents, ScoreRequest.class);
        return scoreRequest;
    }

    protected BulkRecordScoreRequest getBulkScoreRequestForScoreCorrectness() throws IOException {
        BulkRecordScoreRequest scoreRequest = JsonUtils.deserialize(bulkRecordInputForErrorCorrectnessTest,
                BulkRecordScoreRequest.class);
        return scoreRequest;
    }

    protected List<ScoreRequest> getScoreRequestsForScoreCorrectness() throws IOException {
        List<ScoreRequest> scoreRequests = new ArrayList<>();
        ScoreRequest scoreRequest = JsonUtils.deserialize(singleRecordInput1ForErrorCorrectnessTest,
                ScoreRequest.class);
        scoreRequests.add(scoreRequest);
        scoreRequest = JsonUtils.deserialize(singleRecordInput2ForErrorCorrectnessTest, ScoreRequest.class);
        scoreRequests.add(scoreRequest);
        scoreRequest = JsonUtils.deserialize(singleRecordInput3ForErrorCorrectnessTest, ScoreRequest.class);
        scoreRequests.add(scoreRequest);
        scoreRequest = JsonUtils.deserialize(singleRecordInput4ForErrorCorrectnessTest, ScoreRequest.class);
        scoreRequests.add(scoreRequest);
        return scoreRequests;
    }

    protected List<Integer> getExpectedScoresForScoreCorrectness() {
        List<Integer> expectedScores = new ArrayList<>();
        expectedScores.add(99);
        expectedScores.add(47);
        expectedScores.add(89);
        expectedScores.add(88);
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

    private static String singleRecordInput1ForErrorCorrectnessTest = //
            "    {\"modelId\":\"ms__TestInternal3MulesoftAllRows220160314_112802_\",\"source\":\"APIConsole\",\"performEnrichment\":false,\"rule\":\"manual\","
                    + //
                    "    \"record\":{" + //
                    "    \"HasCEDownload\":\"false\",\"Activity_Count_Click_Email\":\"2.0\",\"HasAnypointLogin\":\"false\",\"Activity_Count_Click_Link\":\"12.0\",\"Activity_Count_Interesting_Moment_Any\":\"8.0\",\"Activity_Count_Interesting_Moment_Webinar\":\"0.0\",\"kickboxAcceptAll\":null,\"Activity_Count_Interesting_Moment_Event\":\"0.0\",\"LastName\":\"Dowbor\",\"IsClosed\":null,\"Activity_Count_Interesting_Moment_Pricing\":\"0.0\",\"FirstName\":\"Alexandre\",\"Unsubscribed\":null,\"kickboxStatus\":\"valid\",\"SICCode\":null,\"StageName\":null,\"Activity_Count_Interesting_Moment_Search\":\"0.0\",\"Activity_Count_Interesting_Moment_key_web_page\":\"0.0\",\"Interest_tcat__c\":null,\"Event\":\"false\",\"Lead_Source_Asset__c\":\"Whitepaper - Financial Services Digital Transformation\",\"kickboxDisposable\":null,\"PhoneNumber\":\"6472036970\",\"Source_Detail__c\":null,\"CompanyName\":\"Bank of Montreal\",\"Free_Email_Address__c\":\"true\",\"Activity_Count_Interesting_Moment_Multiple\":\"0.0\",\"Country\":\"Canada\",\"Activity_Count_Interesting_Moment_Email\":\"2.0\",\"Activity_Count_Visit_Webpage\":\"18.0\",\"Title\":\"Sr Business Technology Specialist\",\"City\":\"Toronto\",\"HasEEDownload\":\"false\",\"Interest_esb__c\":\"true\",\"Activity_Count_Open_Email\":\"3.0\",\"InternalId\":\"9053\",\"State\":\"ON\",\"Email\":\"alexdowbor@gmail.com\",\"kickboxFree\":\"true\",\"SourceColumn\":null,\"Activity_Count_Email_Bounced_Soft\":\"0.0\",\"CreatedDate\":\"1453754541000\",\"Id\":\"14091144\",\"Activity_Count_Unsubscribe_Email\":\"0.0\",\"Cloud_Plan__c\":null,\"Activity_Count_Fill_Out_Form\":\"4.0\",\"Industry\":\"Financial Services\""
                    + //
                    "    }" + //
                    "    }";

    private static String singleRecordInput2ForErrorCorrectnessTest = //
            "    {\"modelId\":\"ms__TestInternal3MulesoftAllRows220160314_112802_\",\"source\":\"APIConsole\",\"performEnrichment\":false,\"rule\":\"manual\","
                    + //
                    "    \"record\":{" + //
                    "    \"HasCEDownload\":\"true\",\"Activity_Count_Click_Email\":\"0\",\"HasAnypointLogin\":\"true\",\"Activity_Count_Click_Link\":\"5\",\"Activity_Count_Interesting_Moment_Any\":\"0\",\"Activity_Count_Interesting_Moment_Webinar\":\"0.0\",\"kickboxAcceptAll\":null,\"Activity_Count_Interesting_Moment_Event\":\"0.0\",\"LastName\":\"Jobs\",\"IsClosed\":null,\"Activity_Count_Interesting_Moment_Pricing\":\"0.0\",\"FirstName\":\"Steve\",\"Unsubscribed\":null,\"kickboxStatus\":\"invalid\",\"SICCode\":null,\"StageName\":null,\"Activity_Count_Interesting_Moment_Search\":\"0.0\",\"Activity_Count_Interesting_Moment_key_web_page\":\"1000.0\",\"Interest_tcat__c\":null,\"Event\":\"true\",\"Lead_Source_Asset__c\":\"\",\"kickboxDisposable\":null,\"PhoneNumber\":\"999 999 9999\",\"Source_Detail__c\":null,\"CompanyName\":\"solyndra\",\"Free_Email_Address__c\":\"false\",\"Activity_Count_Interesting_Moment_Multiple\":\"0.0\",\"Country\":\"China\",\"Activity_Count_Interesting_Moment_Email\":\"1.0\",\"Activity_Count_Visit_Webpage\":\"0.0\",\"Title\":null,\"City\":\"Vegas\",\"HasEEDownload\":\"true\",\"Interest_esb__c\":\"true\",\"Activity_Count_Open_Email\":\"0.0\",\"InternalId\":\"5236\",\"State\":\"Texas\",\"Email\":\"steve@solyndra.com\",\"kickboxFree\":\"false\",\"SourceColumn\":null,\"Activity_Count_Email_Bounced_Soft\":\"0.0\",\"CreatedDate\":\"1389754813000\",\"Id\":\"1111\",\"Activity_Count_Unsubscribe_Email\":\"0.0\",\"Cloud_Plan__c\":null,\"Activity_Count_Fill_Out_Form\":\"0.0\",\"Industry\":\"\"}"
                    + //
                    "    }";

    private static String singleRecordInput3ForErrorCorrectnessTest = //
            "    {\"modelId\":\"ms__TestInternal3MulesoftAllRows220160314_112802_\",\"source\":\"APIConsole\",\"performEnrichment\":false,\"rule\":\"manual\","
                    + //
                    "    \"record\":{" + //
                    "    \"HasCEDownload\":\"false\",\"Activity_Count_Click_Email\":\"0.0\",\"HasAnypointLogin\":\"false\",\"Activity_Count_Click_Link\":\"0.0\",\"Activity_Count_Interesting_Moment_Any\":\"0.0\",\"Activity_Count_Interesting_Moment_Webinar\":\"0.0\",\"kickboxAcceptAll\":\"true\",\"Activity_Count_Interesting_Moment_Event\":\"0.0\",\"LastName\":\"Schoger\",\"IsClosed\":null,\"Activity_Count_Interesting_Moment_Pricing\":\"0.0\",\"FirstName\":\"John\",\"Unsubscribed\":null,\"kickboxStatus\":\"valid\",\"SICCode\":null,\"StageName\":null,\"Activity_Count_Interesting_Moment_Search\":\"0.0\",\"Activity_Count_Interesting_Moment_key_web_page\":\"0.0\",\"Interest_tcat__c\":null,\"Event\":\"false\",\"Lead_Source_Asset__c\":\"Inbound Phone/Email/Chat\",\"kickboxDisposable\":null,\"PhoneNumber\":\"(212) 906-0130\",\"Source_Detail__c\":null,\"CompanyName\":\"Strategas\",\"Free_Email_Address__c\":null,\"Activity_Count_Interesting_Moment_Multiple\":\"0.0\",\"Country\":\"United States\",\"Activity_Count_Interesting_Moment_Email\":\"0.0\",\"Activity_Count_Visit_Webpage\":\"0.0\",\"Title\":\"Managing Director, Head of Corporate Services\",\"City\":\"New York\",\"HasEEDownload\":\"false\",\"Interest_esb__c\":null,\"Activity_Count_Open_Email\":\"0.0\",\"InternalId\":\"987\",\"State\":\"NY\",\"Email\":\"jschoger@strategasrp.com\",\"kickboxFree\":null,\"SourceColumn\":null,\"Activity_Count_Email_Bounced_Soft\":\"0.0\",\"CreatedDate\":\"1437511481000\",\"Id\":\"1713466\",\"Activity_Count_Unsubscribe_Email\":\"0.0\",\"Cloud_Plan__c\":null,\"Activity_Count_Fill_Out_Form\":\"0.0\",\"Industry\":null"
                    + //
                    "    }" + //
                    "    }";

    private static String singleRecordInput4ForErrorCorrectnessTest = //
            "    {\"modelId\":\"ms__TestInternal3MulesoftAllRows220160314_112802_\",\"source\":\"APIConsole\",\"performEnrichment\":false,\"rule\":\"manual\","
                    + //
                    "    \"record\":{" + //
                    "    \"HasCEDownload\":\"false\",\"Activity_Count_Click_Email\":\"1.0\",\"HasAnypointLogin\":\"false\",\"Activity_Count_Click_Link\":\"0.0\",\"Activity_Count_Interesting_Moment_Any\":\"1.0\",\"Activity_Count_Interesting_Moment_Webinar\":\"0.0\",\"kickboxAcceptAll\":null,\"Activity_Count_Interesting_Moment_Event\":\"0.0\",\"LastName\":\"Cox\",\"IsClosed\":null,\"Activity_Count_Interesting_Moment_Pricing\":\"0.0\",\"FirstName\":\"Landon\",\"Unsubscribed\":null,\"kickboxStatus\":\"valid\",\"SICCode\":null,\"StageName\":null,\"Activity_Count_Interesting_Moment_Search\":\"0.0\",\"Activity_Count_Interesting_Moment_key_web_page\":\"0.0\",\"Interest_tcat__c\":null,\"Event\":\"false\",\"Lead_Source_Asset__c\":\"eBook - Becoming a Customer Company\",\"kickboxDisposable\":null,\"PhoneNumber\":\"817 375 8606\",\"Source_Detail__c\":null,\"CompanyName\":\"VIP Villas\",\"Free_Email_Address__c\":\"true\",\"Activity_Count_Interesting_Moment_Multiple\":\"0.0\",\"Country\":\"United States\",\"Activity_Count_Interesting_Moment_Email\":\"1.0\",\"Activity_Count_Visit_Webpage\":\"0.0\",\"Title\":null,\"City\":\"Dallas\",\"HasEEDownload\":\"false\",\"Interest_esb__c\":\"true\",\"Activity_Count_Open_Email\":\"0.0\",\"InternalId\":\"5236\",\"State\":\"Texas\",\"Email\":\"jhcox11@gmail.com\",\"kickboxFree\":\"true\",\"SourceColumn\":null,\"Activity_Count_Email_Bounced_Soft\":\"0.0\",\"CreatedDate\":\"1389754813000\",\"Id\":\"5878228\",\"Activity_Count_Unsubscribe_Email\":\"0.0\",\"Cloud_Plan__c\":null,\"Activity_Count_Fill_Out_Form\":\"0.0\",\"Industry\":null}"
                    + //
                    "    }";

    private static String bulkRecordInputForErrorCorrectnessTest = //
            "    {\"source\":\"Dummy Source\",\"records\":[" + //
                    "    {\"recordId\":\"2\",\"idType\":\"LATTICE\"," + //
                    "    \"modelAttributeValuesMap\":" + //
                    "    {\"ms__TestInternal3MulesoftAllRows220160314_112802_\":{" + //
                    "    \"HasCEDownload\":\"false\",\"Activity_Count_Click_Email\":\"2.0\",\"HasAnypointLogin\":\"false\",\"Activity_Count_Click_Link\":\"12.0\",\"Activity_Count_Interesting_Moment_Any\":\"8.0\",\"Activity_Count_Interesting_Moment_Webinar\":\"0.0\",\"kickboxAcceptAll\":null,\"Activity_Count_Interesting_Moment_Event\":\"0.0\",\"LastName\":\"Dowbor\",\"IsClosed\":null,\"Activity_Count_Interesting_Moment_Pricing\":\"0.0\",\"FirstName\":\"Alexandre\",\"Unsubscribed\":null,\"kickboxStatus\":\"valid\",\"SICCode\":null,\"StageName\":null,\"Activity_Count_Interesting_Moment_Search\":\"0.0\",\"Activity_Count_Interesting_Moment_key_web_page\":\"0.0\",\"Interest_tcat__c\":null,\"Event\":\"false\",\"Lead_Source_Asset__c\":\"Whitepaper - Financial Services Digital Transformation\",\"kickboxDisposable\":null,\"PhoneNumber\":\"6472036970\",\"Source_Detail__c\":null,\"CompanyName\":\"Bank of Montreal\",\"Free_Email_Address__c\":\"true\",\"Activity_Count_Interesting_Moment_Multiple\":\"0.0\",\"Country\":\"Canada\",\"Activity_Count_Interesting_Moment_Email\":\"2.0\",\"Activity_Count_Visit_Webpage\":\"18.0\",\"Title\":\"Sr Business Technology Specialist\",\"City\":\"Toronto\",\"HasEEDownload\":\"false\",\"Interest_esb__c\":\"true\",\"Activity_Count_Open_Email\":\"3.0\",\"InternalId\":\"9053\",\"State\":\"ON\",\"Email\":\"alexdowbor@gmail.com\",\"kickboxFree\":\"true\",\"SourceColumn\":null,\"Activity_Count_Email_Bounced_Soft\":\"0.0\",\"CreatedDate\":\"1453754541000\",\"Id\":\"14091144\",\"Activity_Count_Unsubscribe_Email\":\"0.0\",\"Cloud_Plan__c\":null,\"Activity_Count_Fill_Out_Form\":\"4.0\",\"Industry\":\"Financial Services\"}"
                    + //
                    "    }" + //
                    "    }," + //
                    "    {\"recordId\":\"4\",\"idType\":\"LATTICE\"," + //
                    "    \"modelAttributeValuesMap\":" + //
                    "    {\"ms__TestInternal3MulesoftAllRows220160314_112802_\":{\"HasCEDownload\":\"true\",\"Activity_Count_Click_Email\":\"0\",\"HasAnypointLogin\":\"true\",\"Activity_Count_Click_Link\":\"5\",\"Activity_Count_Interesting_Moment_Any\":\"0\",\"Activity_Count_Interesting_Moment_Webinar\":\"0.0\",\"kickboxAcceptAll\":null,\"Activity_Count_Interesting_Moment_Event\":\"0.0\",\"LastName\":\"Jobs\",\"IsClosed\":null,\"Activity_Count_Interesting_Moment_Pricing\":\"0.0\",\"FirstName\":\"Steve\",\"Unsubscribed\":null,\"kickboxStatus\":\"invalid\",\"SICCode\":null,\"StageName\":null,\"Activity_Count_Interesting_Moment_Search\":\"0.0\",\"Activity_Count_Interesting_Moment_key_web_page\":\"1000.0\",\"Interest_tcat__c\":null,\"Event\":\"true\",\"Lead_Source_Asset__c\":\"\",\"kickboxDisposable\":null,\"PhoneNumber\":\"999 999 9999\",\"Source_Detail__c\":null,\"CompanyName\":\"solyndra\",\"Free_Email_Address__c\":\"false\",\"Activity_Count_Interesting_Moment_Multiple\":\"0.0\",\"Country\":\"China\",\"Activity_Count_Interesting_Moment_Email\":\"1.0\",\"Activity_Count_Visit_Webpage\":\"0.0\",\"Title\":null,\"City\":\"Vegas\",\"HasEEDownload\":\"true\",\"Interest_esb__c\":\"true\",\"Activity_Count_Open_Email\":\"0.0\",\"InternalId\":\"5236\",\"State\":\"Texas\",\"Email\":\"steve@solyndra.com\",\"kickboxFree\":\"false\",\"SourceColumn\":null,\"Activity_Count_Email_Bounced_Soft\":\"0.0\",\"CreatedDate\":\"1389754813000\",\"Id\":\"1111\",\"Activity_Count_Unsubscribe_Email\":\"0.0\",\"Cloud_Plan__c\":null,\"Activity_Count_Fill_Out_Form\":\"0.0\",\"Industry\":\"\"}"
                    + //
                    "    }" + //
                    "    }," + //
                    "    {\"recordId\":\"3\",\"idType\":\"LATTICE\"," + //
                    "    \"modelAttributeValuesMap\":" + //
                    "    {\"ms__TestInternal3MulesoftAllRows220160314_112802_\":{" + //
                    "    " + //
                    "    \"HasCEDownload\":\"false\",\"Activity_Count_Click_Email\":\"0.0\",\"HasAnypointLogin\":\"false\",\"Activity_Count_Click_Link\":\"0.0\",\"Activity_Count_Interesting_Moment_Any\":\"0.0\",\"Activity_Count_Interesting_Moment_Webinar\":\"0.0\",\"kickboxAcceptAll\":\"true\",\"Activity_Count_Interesting_Moment_Event\":\"0.0\",\"LastName\":\"Schoger\",\"IsClosed\":null,\"Activity_Count_Interesting_Moment_Pricing\":\"0.0\",\"FirstName\":\"John\",\"Unsubscribed\":null,\"kickboxStatus\":\"valid\",\"SICCode\":null,\"StageName\":null,\"Activity_Count_Interesting_Moment_Search\":\"0.0\",\"Activity_Count_Interesting_Moment_key_web_page\":\"0.0\",\"Interest_tcat__c\":null,\"Event\":\"false\",\"Lead_Source_Asset__c\":\"Inbound Phone/Email/Chat\",\"kickboxDisposable\":null,\"PhoneNumber\":\"(212) 906-0130\",\"Source_Detail__c\":null,\"CompanyName\":\"Strategas\",\"Free_Email_Address__c\":null,\"Activity_Count_Interesting_Moment_Multiple\":\"0.0\",\"Country\":\"United States\",\"Activity_Count_Interesting_Moment_Email\":\"0.0\",\"Activity_Count_Visit_Webpage\":\"0.0\",\"Title\":\"Managing Director, Head of Corporate Services\",\"City\":\"New York\",\"HasEEDownload\":\"false\",\"Interest_esb__c\":null,\"Activity_Count_Open_Email\":\"0.0\",\"InternalId\":\"987\",\"State\":\"NY\",\"Email\":\"jschoger@strategasrp.com\",\"kickboxFree\":null,\"SourceColumn\":null,\"Activity_Count_Email_Bounced_Soft\":\"0.0\",\"CreatedDate\":\"1437511481000\",\"Id\":\"1713466\",\"Activity_Count_Unsubscribe_Email\":\"0.0\",\"Cloud_Plan__c\":null,\"Activity_Count_Fill_Out_Form\":\"0.0\",\"Industry\":null}"
                    + //
                    "    }" + //
                    "    }," + //
                    "    {\"recordId\":\"36c5c666-1eb4-4221-b5d3-93b23be72a6e\",\"idType\":\"LATTICE\"," + //
                    "    \"modelAttributeValuesMap\":" + //
                    "    {\"ms__TestInternal3MulesoftAllRows220160314_112802_\":{\"HasCEDownload\":\"false\",\"Activity_Count_Click_Email\":\"1.0\",\"HasAnypointLogin\":\"false\",\"Activity_Count_Click_Link\":\"0.0\",\"Activity_Count_Interesting_Moment_Any\":\"1.0\",\"Activity_Count_Interesting_Moment_Webinar\":\"0.0\",\"kickboxAcceptAll\":null,\"Activity_Count_Interesting_Moment_Event\":\"0.0\",\"LastName\":\"Cox\",\"IsClosed\":null,\"Activity_Count_Interesting_Moment_Pricing\":\"0.0\",\"FirstName\":\"Landon\",\"Unsubscribed\":null,\"kickboxStatus\":\"valid\",\"SICCode\":null,\"StageName\":null,\"Activity_Count_Interesting_Moment_Search\":\"0.0\",\"Activity_Count_Interesting_Moment_key_web_page\":\"0.0\",\"Interest_tcat__c\":null,\"Event\":\"false\",\"Lead_Source_Asset__c\":\"eBook - Becoming a Customer Company\",\"kickboxDisposable\":null,\"PhoneNumber\":\"817 375 8606\",\"Source_Detail__c\":null,\"CompanyName\":\"VIP Villas\",\"Free_Email_Address__c\":\"true\",\"Activity_Count_Interesting_Moment_Multiple\":\"0.0\",\"Country\":\"United States\",\"Activity_Count_Interesting_Moment_Email\":\"1.0\",\"Activity_Count_Visit_Webpage\":\"0.0\",\"Title\":null,\"City\":\"Dallas\",\"HasEEDownload\":\"false\",\"Interest_esb__c\":\"true\",\"Activity_Count_Open_Email\":\"0.0\",\"InternalId\":\"5236\",\"State\":\"Texas\",\"Email\":\"jhcox11@gmail.com\",\"kickboxFree\":\"true\",\"SourceColumn\":null,\"Activity_Count_Email_Bounced_Soft\":\"0.0\",\"CreatedDate\":\"1389754813000\",\"Id\":\"5878228\",\"Activity_Count_Unsubscribe_Email\":\"0.0\",\"Cloud_Plan__c\":null,\"Activity_Count_Fill_Out_Form\":\"0.0\",\"Industry\":null}"
                    + //
                    "    }" + //
                    "    }" + //
                    "    ]" + //
                    "    }";

    private static String pmmlImputJson = //
            "{" + //
                    "    \"modelId\": \"ms__9f990ce6-a412-4cb1-aaf8-0b2313eda3ea-PMMLMode\"," + //
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
