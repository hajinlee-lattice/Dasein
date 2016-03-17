package com.latticeengines.scoringapi.functionalframework;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.oauth2.client.OAuth2RestTemplate;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.google.common.io.Files;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.oauth.OAuthUser;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.scoringapi.DataComposition;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.oauth2db.exposed.entitymgr.OAuthUserEntityMgr;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;
import com.latticeengines.scoringapi.controller.InternalResourceRestApiProxy;
import com.latticeengines.scoringapi.exposed.ScoreRequest;
import com.latticeengines.scoringapi.model.impl.ModelRetrieverImpl;
import com.latticeengines.testframework.domain.pls.ModelSummaryUtils;

public class ScoringApiControllerTestNGBase extends ScoringApiFunctionalTestNGBase {

    protected static final String TEST_MODEL_FOLDERNAME = "3MulesoftAllRows20160314_112802";

    protected static final String MODEL_ID = "ms__" + TEST_MODEL_FOLDERNAME +"_";
    protected static final String LOCAL_MODEL_PATH = "com/latticeengines/scoringapi/model/" + TEST_MODEL_FOLDERNAME +"/";
    protected static final String TENANT_ID = "ScoringApiTestTenant.ScoringApiTestTenant.Production";
    protected static final String APPLICATION_ID = "application_1457046993615_3821";
    protected static final String PARSED_APPLICATION_ID = "1457046993615_3821";
    protected static final String MODEL_VERSION = "8ba99b36-c222-4f93-ab8a-6dcc11ce45e9";
    protected static final String EVENT_TABLE = TEST_MODEL_FOLDERNAME;
    protected static final String SOURCE_INTERPRETATION = "SalesforceLead";
    protected static final CustomerSpace customerSpace = CustomerSpace.parse(TENANT_ID);
    private static final String MODELSUMMARYJSON_LOCALPATH = LOCAL_MODEL_PATH + ModelRetrieverImpl.MODEL_JSON;
    private static final Log log = LogFactory.getLog(ScoringApiControllerTestNGBase.class);

    private static final String CLIENT_ID_LP = "lp";

    @Value("${scoringapi.hostport}")
    protected String apiHostPort;

    @Value("${scoringapi.auth.hostport}")
    protected String authHostPort;

    @Value("${scoringapi.playmakerapi.hostport}")
    protected String playMakerApiHostPort;

    @Value("${scoringapi.pls.api.hostport}")
    protected String plsApiHostPort;

    @Autowired
    protected OAuthUserEntityMgr userEntityMgr;

    @Autowired
    protected Configuration yarnConfiguration;

    protected InternalResourceRestApiProxy plsRest = null;

    protected DataComposition eventTableDataComposition;

    protected OAuthUser oAuthUser;

    protected OAuth2RestTemplate oAuth2RestTemplate = null;

    @BeforeClass(groups = "functional")
    public void beforeClass() throws IOException {
        plsRest = new InternalResourceRestApiProxy(plsApiHostPort);
        oAuthUser = getOAuthUser(TENANT_ID);
        oAuth2RestTemplate = OAuth2Utils.getOauthTemplate(authHostPort, oAuthUser.getUserId(), oAuthUser.getPassword(),
                CLIENT_ID_LP);
        OAuth2AccessToken accessToken = oAuth2RestTemplate.getAccessToken();
        log.info(accessToken.getValue());
        Tenant tenant = setupTenantAndModelSummary();
        setupHdfsArtifacts(tenant);
    }

    @AfterClass(groups = "functional")
    public void afterClass() {
        userEntityMgr.delete(oAuthUser.getUserId());
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

    private void setPassword(OAuthUser user, String userId) {
        user.setPassword(OAuth2Utils.generatePassword());
        user.setPasswordExpiration(userEntityMgr.getPasswordExpiration(userId));
    }

    private Tenant setupTenantAndModelSummary() throws IOException {
        String tenantId = TENANT_ID;
        Tenant tenant = new Tenant();
        tenant.setId(tenantId);
        tenant.setName(tenantId);
        plsRest.deleteTenant(customerSpace);
        plsRest.createTenant(tenant);

        ModelSummary modelSummary = ModelSummaryUtils.generateModelSummary(tenant, MODELSUMMARYJSON_LOCALPATH);
        modelSummary.setApplicationId(APPLICATION_ID);
        modelSummary.setEventTableName(EVENT_TABLE);
        modelSummary.setId(MODEL_ID);
        modelSummary.setLookupId(String.format("%s|%s|%s", TENANT_ID, EVENT_TABLE, MODEL_VERSION));
        modelSummary.setSourceSchemaInterpretation(SOURCE_INTERPRETATION);
        modelSummary.setStatus(ModelSummaryStatus.ACTIVE);

        String modelId = modelSummary.getId();
        ModelSummary retrievedSummary = plsRest.getModelSummaryFromModelId(modelId, customerSpace);
        if (retrievedSummary != null) {
            plsRest.deleteModelSummary(modelId, customerSpace);
        }
        plsRest.createModelSummary(modelSummary, customerSpace);

        return tenant;
    }

    private void setupHdfsArtifacts(Tenant tenant) throws IOException {
        String tenantId = tenant.getId();
        String artifactTableDir = String
                .format(ModelRetrieverImpl.HDFS_SCORE_ARTIFACT_TABLE_DIR, tenantId, EVENT_TABLE);
        String artifactBaseDir = String.format(ModelRetrieverImpl.HDFS_SCORE_ARTIFACT_BASE_DIR, tenantId, EVENT_TABLE,
                MODEL_VERSION, PARSED_APPLICATION_ID);
        String enhancementsDir = artifactBaseDir + ModelRetrieverImpl.HDFS_ENHANCEMENTS_DIR;

        URL eventTableDataCompositionUrl = ClassLoader
                .getSystemResource(LOCAL_MODEL_PATH + "metadata-" + ModelRetrieverImpl.DATA_COMPOSITION_FILENAME);
        URL modelJsonUrl = ClassLoader.getSystemResource(MODELSUMMARYJSON_LOCALPATH);
        URL rfpmmlUrl = ClassLoader.getSystemResource(LOCAL_MODEL_PATH + ModelRetrieverImpl.PMML_FILENAME);
        URL dataScienceDataCompositionUrl = ClassLoader
                .getSystemResource(LOCAL_MODEL_PATH + ModelRetrieverImpl.DATA_COMPOSITION_FILENAME);
        URL scoreDerivationUrl = ClassLoader
                .getSystemResource(LOCAL_MODEL_PATH + ModelRetrieverImpl.SCORE_DERIVATION_FILENAME);

        HdfsUtils.rmdir(yarnConfiguration, artifactTableDir);
        HdfsUtils.rmdir(yarnConfiguration, artifactBaseDir);
        HdfsUtils.rmdir(yarnConfiguration, enhancementsDir);

        HdfsUtils.mkdir(yarnConfiguration, artifactTableDir);
        HdfsUtils.mkdir(yarnConfiguration, artifactBaseDir);
        HdfsUtils.mkdir(yarnConfiguration, enhancementsDir);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, eventTableDataCompositionUrl.getFile(), artifactTableDir
                + ModelRetrieverImpl.DATA_COMPOSITION_FILENAME);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelJsonUrl.getFile(), artifactBaseDir
                + TEST_MODEL_FOLDERNAME + "_model.json");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, rfpmmlUrl.getFile(), artifactBaseDir + ModelRetrieverImpl.PMML_FILENAME);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, dataScienceDataCompositionUrl.getFile(), enhancementsDir
                + ModelRetrieverImpl.DATA_COMPOSITION_FILENAME);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, scoreDerivationUrl.getFile(), enhancementsDir
                + ModelRetrieverImpl.SCORE_DERIVATION_FILENAME);

        String eventTableDataCompositionContents = Files.toString(new File(eventTableDataCompositionUrl.getFile()),
                Charset.defaultCharset());
        eventTableDataComposition = JsonUtils.deserialize(eventTableDataCompositionContents, DataComposition.class);
    }

    protected ScoreRequest getScoreRequest() throws IOException {
        URL scoreRequestUrl = ClassLoader.getSystemResource(LOCAL_MODEL_PATH + "score_request.json");
        String scoreRecordContents = Files.toString(new File(scoreRequestUrl.getFile()), Charset.defaultCharset());
        ScoreRequest scoreRequest = JsonUtils.deserialize(scoreRecordContents, ScoreRequest.class);
        return scoreRequest;
    }

}
