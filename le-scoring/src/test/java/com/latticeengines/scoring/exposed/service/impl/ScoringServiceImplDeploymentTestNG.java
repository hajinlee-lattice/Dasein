package com.latticeengines.scoring.exposed.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.scoring.RTSBulkScoringConfiguration;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.scoring.exposed.InternalResourceRestApiProxy;
import com.latticeengines.scoring.functionalframework.ScoringFunctionalTestNGBase;
import com.latticeengines.scoringapi.exposed.model.impl.ModelRetrieverImpl;
import com.latticeengines.testframework.domain.pls.ModelSummaryUtils;

public class ScoringServiceImplDeploymentTestNG extends ScoringFunctionalTestNGBase {

    @Value("${scoring.pls.api.hostport}")
    private String plsApiHostPort;

    @Autowired
    private ScoringServiceImpl scoringService;

    private static final String TEST_INPUT_DATA_DIR = "/Pods/QA/Contracts/ScoringServiceImplTestNG/Tenants/ScoringServiceImplTestNG/Spaces/Production/Data/Tables/";

    private static final String AVRO_FILE_SUFFIX = "File/SourceFile_file_1462229180545_csv/Extracts/2016-05-02-18-47-03/";

    private static final String AVRO_FILE = "part-m-00000.avro";

    private static final String TEST_MODEL_NAME_PREFIX = "a8684c37-a3b9-452f-b7e3-af440e4365b8";

    private static final String LOCAL_DATA_DIR = "com/latticeengines/scoring/rts/data/";

    protected static final String TENANT_ID = "ScoringServiceImplTestNG.ScoringServiceImplTestNG.Production";

    protected static final CustomerSpace customerSpace = CustomerSpace.parse(TENANT_ID);

    protected InternalResourceRestApiProxy plsRest;

    protected Tenant tenant;

    @BeforeClass(groups = "deployment")
    public void setup() throws IOException {
        String testModelFolderName = TEST_MODEL_NAME_PREFIX;
        String applicationId = "application_" + "1457046993615_3823";
        String modelVersion = "157342cb-a8fb-4158-b62a-699441401e9a";
        ScoringTestModelConfiguration modelConfiguration = new ScoringTestModelConfiguration(testModelFolderName,
                applicationId, modelVersion);
        plsRest = new InternalResourceRestApiProxy(plsApiHostPort);
        tenant = setupTenant();
        createModel(plsRest, tenant, modelConfiguration, customerSpace);
        setupHdfsArtifacts(yarnConfiguration, tenant, modelConfiguration);
    }

    @AfterClass(groups = "deployment")
    public void cleanup() throws IOException {
    }

    @Test(groups = "deployment")
    public void testSubmitScoringYarnContainerJob() throws Exception {
        RTSBulkScoringConfiguration rtsBulkScoringConfig = new RTSBulkScoringConfiguration();
        rtsBulkScoringConfig.setCustomerSpace(customerSpace);
        rtsBulkScoringConfig.setTenant(customerSpace.getTenantId().toString());
        List<String> modelGuids = new ArrayList<String>();
        modelGuids.add("ms__a8684c37-a3b9-452f-b7e3-af440e4365b8_");
        rtsBulkScoringConfig.setModelGuids(modelGuids);
        Table metadataTable = new Table();
        Extract extract = new Extract();
        extract.setPath(TEST_INPUT_DATA_DIR + AVRO_FILE_SUFFIX + AVRO_FILE);
        metadataTable.addExtract(extract);
        rtsBulkScoringConfig.setMetadataTable(metadataTable);
        String tableName = String.format("RTSBulkScoreResult_%s_%d", TEST_MODEL_NAME_PREFIX.replaceAll("-", "_"),
                System.currentTimeMillis());
        String targeDir = TEST_INPUT_DATA_DIR + tableName;
        rtsBulkScoringConfig.setTargetResultDir(targeDir);
        ApplicationId appId = scoringService.submitScoreWorkflow(rtsBulkScoringConfig);
        assertNotNull(appId);
        System.out.println(appId);
        FinalApplicationStatus status = waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

    private Tenant setupTenant() throws IOException {
        String tenantId = TENANT_ID;
        Tenant tenant = new Tenant();
        tenant.setId(tenantId);
        tenant.setName(tenantId);
        plsRest.deleteTenant(customerSpace);
        plsRest.createTenant(tenant);
        return tenant;
    }

    private ModelSummary createModel(InternalResourceRestApiProxy plsRest, Tenant tenant,
            ScoringTestModelConfiguration modelConfiguration, CustomerSpace customerSpace) throws IOException {
        ModelSummary modelSummary = ModelSummaryUtils.generateModelSummary(tenant,
                modelConfiguration.getModelSummaryJsonLocalpath());
        modelSummary.setApplicationId(modelConfiguration.getApplicationId());
        modelSummary.setEventTableName(modelConfiguration.getEventTable());
        modelSummary.setId(modelConfiguration.getModelId());
        modelSummary.setName(modelConfiguration.getModelName());
        modelSummary.setDisplayName(modelConfiguration.getModelName());
        modelSummary.setLookupId(String.format("%s|%s|%s", tenant.getId(), modelConfiguration.getEventTable(),
                modelConfiguration.getModelVersion()));
        modelSummary.setSourceSchemaInterpretation(modelConfiguration.getSourceInterpretation());
        modelSummary.setStatus(ModelSummaryStatus.ACTIVE);

        ModelSummary retrievedSummary = plsRest.getModelSummaryFromModelId(modelConfiguration.getModelId(),
                customerSpace);
        if (retrievedSummary != null) {
            plsRest.deleteModelSummary(modelConfiguration.getModelId(), customerSpace);
        }
        plsRest.createModelSummary(modelSummary, customerSpace);
        return modelSummary;
    }

    private void setupHdfsArtifacts(Configuration yarnConfiguration, Tenant tenant,
            ScoringTestModelConfiguration modelConfiguration) throws IOException {
        String tenantId = tenant.getId();
        String artifactTableDir = String.format(ModelRetrieverImpl.HDFS_SCORE_ARTIFACT_EVENTTABLE_DIR, tenantId,
                modelConfiguration.getEventTable());
        String artifactBaseDir = String.format(ModelRetrieverImpl.HDFS_SCORE_ARTIFACT_BASE_DIR, tenantId,
                modelConfiguration.getEventTable(), modelConfiguration.getModelVersion(),
                modelConfiguration.getParsedApplicationId());
        String enhancementsDir = artifactBaseDir + ModelRetrieverImpl.HDFS_ENHANCEMENTS_DIR;
        String inputDataDir = TEST_INPUT_DATA_DIR + AVRO_FILE_SUFFIX;

        URL dataCompositionUrl = ClassLoader.getSystemResource(modelConfiguration.getLocalModelPath()
                + ModelRetrieverImpl.DATA_COMPOSITION_FILENAME);
        URL modelJsonUrl = ClassLoader.getSystemResource(modelConfiguration.getModelSummaryJsonLocalpath());
        URL rfpmmlUrl = ClassLoader.getSystemResource(modelConfiguration.getLocalModelPath()
                + ModelRetrieverImpl.PMML_FILENAME);
        URL scoreDerivationUrl = ClassLoader.getSystemResource(modelConfiguration.getLocalModelPath()
                + ModelRetrieverImpl.SCORE_DERIVATION_FILENAME);
        URL inputAvroFile = ClassLoader.getSystemResource(LOCAL_DATA_DIR + AVRO_FILE);

        HdfsUtils.rmdir(yarnConfiguration, artifactTableDir);
        HdfsUtils.rmdir(yarnConfiguration, artifactBaseDir);
        HdfsUtils.rmdir(yarnConfiguration, enhancementsDir);
        HdfsUtils.rmdir(yarnConfiguration, TEST_INPUT_DATA_DIR);

        HdfsUtils.mkdir(yarnConfiguration, artifactTableDir);
        HdfsUtils.mkdir(yarnConfiguration, artifactBaseDir);
        HdfsUtils.mkdir(yarnConfiguration, enhancementsDir);
        HdfsUtils.mkdir(yarnConfiguration, inputDataDir);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, dataCompositionUrl.getFile(), artifactTableDir
                + ModelRetrieverImpl.DATA_COMPOSITION_FILENAME);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelJsonUrl.getFile(),
                artifactBaseDir + modelConfiguration.getTestModelFolderName() + "_model.json");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, rfpmmlUrl.getFile(), artifactBaseDir
                + ModelRetrieverImpl.PMML_FILENAME);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, scoreDerivationUrl.getFile(), enhancementsDir
                + ModelRetrieverImpl.SCORE_DERIVATION_FILENAME);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, dataCompositionUrl.getFile(), enhancementsDir
                + ModelRetrieverImpl.DATA_COMPOSITION_FILENAME);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, inputAvroFile.getFile(), inputDataDir);
    }

    public class ScoringTestModelConfiguration {
        private String testModelFolderName;
        private String modelId;
        private String modelName;
        private String localModelPath = "com/latticeengines/scoring/rts/model/";
        private String applicationId;
        private String parsedApplicationId;
        private String modelVersion;
        private String eventTable;
        private String sourceInterpretation;
        private String modelSummaryJsonLocalpath;

        public ScoringTestModelConfiguration(String testModelFolderName, String applicationId, String modelVersion) {
            this.testModelFolderName = testModelFolderName;
            this.modelId = "ms__" + testModelFolderName + "_";
            this.modelName = testModelFolderName;
            this.applicationId = applicationId;
            this.parsedApplicationId = applicationId.substring(applicationId.indexOf("_") + 1);
            this.modelVersion = modelVersion;
            this.eventTable = testModelFolderName;
            this.sourceInterpretation = "SalesforceLead";
            this.modelSummaryJsonLocalpath = localModelPath + ModelRetrieverImpl.MODEL_JSON;
        }

        public String getTestModelFolderName() {
            return testModelFolderName;
        }

        public String getModelId() {
            return modelId;
        }

        public String getModelName() {
            return modelName;
        }

        public String getLocalModelPath() {
            return localModelPath;
        }

        public String getApplicationId() {
            return applicationId;
        }

        public String getParsedApplicationId() {
            return parsedApplicationId;
        }

        public String getModelVersion() {
            return modelVersion;
        }

        public String getEventTable() {
            return eventTable;
        }

        public String getSourceInterpretation() {
            return sourceInterpretation;
        }

        public String getModelSummaryJsonLocalpath() {
            return modelSummaryJsonLocalpath;
        }
    }

}
