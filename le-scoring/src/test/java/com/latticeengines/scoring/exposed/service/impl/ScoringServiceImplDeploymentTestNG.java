package com.latticeengines.scoring.exposed.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttributesOperationMap;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.pls.ModelType;
import com.latticeengines.domain.exposed.scoring.RTSBulkScoringConfiguration;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.lp.CreateBucketMetadataRequest;
import com.latticeengines.proxy.exposed.lp.BucketedScoreProxy;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.pls.PlsInternalProxy;
import com.latticeengines.scoring.functionalframework.ScoringFunctionalTestNGBase;
import com.latticeengines.scoring.util.ScoringTestUtils;
import com.latticeengines.scoringapi.exposed.model.ModelJsonTypeHandler;
import com.latticeengines.scoringapi.exposed.model.impl.ModelRetrieverImpl;
import com.latticeengines.testframework.exposed.utils.ModelSummaryUtils;
import com.latticeengines.testframework.service.impl.GlobalAuthDeploymentTestBed;

public class ScoringServiceImplDeploymentTestNG extends ScoringFunctionalTestNGBase {

    @Value("${common.test.pls.url}")
    private String plsApiHostPort;

    @Inject
    private ScoringServiceImpl scoringService;

    @Inject
    @Qualifier(value = "deploymentTestBed")
    protected GlobalAuthDeploymentTestBed deploymentTestBed;

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    @Inject
    private BucketedScoreProxy bucketedScoreProxy;

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    @Inject
    private PlsInternalProxy plsInternalProxy;

    private static String TEST_INPUT_DATA_DIR;

    private static final String AVRO_FILE_SUFFIX = "File/SourceFile_file_1462229180545_csv/Extracts/2016-05-02-18-47-03/";

    private static final String AVRO_FILE = "part-m-00000_small.avro";

    private static String TEST_MODEL_NAME_PREFIX;

    private static final String LOCAL_DATA_DIR = "com/latticeengines/scoring/rts/data/";

    protected static String TENANT_ID;

    protected static String targetDir;

    protected static CustomerSpace customerSpace;

    protected Tenant tenant;

    private String artifactTableDir;
    private String artifactBaseDir;
    private String enhancementsDir;

    @BeforeClass(groups = "deployment")
    public void setup() throws IOException {
        tenant = deploymentTestBed.bootstrapForProduct(LatticeProduct.LPA3);
        TENANT_ID = tenant.getId();
        deploymentTestBed.switchToSuperAdmin();
        customerSpace = CustomerSpace.parse(TENANT_ID);
        System.out.println(customerSpace.toString());
        TEST_INPUT_DATA_DIR = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId().toString(), customerSpace)
                .toString();

        TEST_MODEL_NAME_PREFIX = UUID.randomUUID().toString();
        String testModelFolderName = TEST_MODEL_NAME_PREFIX;
        String applicationId = "application_" + "1457046993615_3823";
        String modelVersion = "157342cb-a8fb-4158-b62a-699441401e9a";
        ScoringTestModelConfiguration modelConfiguration = new ScoringTestModelConfiguration(testModelFolderName,
                applicationId, modelVersion);
        ModelSummary modelSummary = createModel(modelSummaryProxy, tenant, modelConfiguration, customerSpace);
        generateDefaultBucketMetadata(modelSummary, customerSpace);
        setupHdfsArtifacts(yarnConfiguration, tenant, modelConfiguration);
        saveAttributeSelection(customerSpace);
    }

    @AfterClass(groups = "deployment")
    public void cleanup() throws IOException {
        deploymentTestBed.deleteTenant(tenant);
    }

    @Test(groups = "deployment")
    public void testSubmitScoringYarnContainerJob() throws Exception {
        RTSBulkScoringConfiguration rtsBulkScoringConfig = new RTSBulkScoringConfiguration();
        rtsBulkScoringConfig.setCustomerSpace(customerSpace);
        List<String> modelGuids = new ArrayList<String>();
        modelGuids.add("ms__" + TEST_MODEL_NAME_PREFIX + "_");
        rtsBulkScoringConfig.setModelGuids(modelGuids);
        Table metadataTable = new Table();
        Extract extract = new Extract();
        extract.setPath(TEST_INPUT_DATA_DIR + AVRO_FILE_SUFFIX + AVRO_FILE);
        metadataTable.addExtract(extract);
        rtsBulkScoringConfig.setMetadataTable(metadataTable);
        String tableName = String.format("RTSBulkScoreResult_%s_%d", TEST_MODEL_NAME_PREFIX.replaceAll("-", "_"),
                System.currentTimeMillis());
        targetDir = TEST_INPUT_DATA_DIR + tableName;
        rtsBulkScoringConfig.setTargetResultDir(targetDir);
        rtsBulkScoringConfig.setEnableLeadEnrichment(true);
        rtsBulkScoringConfig.setScoreTestFile(true);
        rtsBulkScoringConfig.setEnableDebug(false);
        rtsBulkScoringConfig.setModelType(ModelType.PYTHONMODEL.getModelType());
        rtsBulkScoringConfig.setInternalResourceHostPort(plsApiHostPort);
        ApplicationId appId = scoringService.submitScoreWorkflow(rtsBulkScoringConfig);
        assertNotNull(appId);
        System.out.println(appId);
        FinalApplicationStatus status = waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
        testHdfsFile();
    }

    private void testHdfsFile() throws IllegalArgumentException, Exception {
        List<String> avroFiles = HdfsUtils.getFilesForDir(yarnConfiguration, targetDir, ".*.avro$");
        Assert.assertNotNull(avroFiles);
        Assert.assertEquals(avroFiles.size(), 1);
        String scoreContents = HdfsUtils.getHdfsFileContents(yarnConfiguration, avroFiles.get(0));
        Assert.assertNotNull(scoreContents);
        List<GenericRecord> list = AvroUtils.getData(yarnConfiguration, new Path(avroFiles.get(0)));
        for (GenericRecord record : list) {
            System.out.println(record);
            Assert.assertNotNull(record.get(InterfaceName.Id.toString()));
            Assert.assertNotNull(record.get(ScoreResultField.ModelId.displayName));
            Assert.assertNotNull(record.get(ScoreResultField.Percentile.displayName));
            Assert.assertNotNull(record.get(ScoreResultField.Rating.displayName));
            Assert.assertNotNull(record.get(ScoreResultField.RawScore.displayName));
        }
        Assert.assertEquals(list.size(), 20);

        GenericRecord record = list.get(0);
        Schema schema = record.getSchema();
        List<Schema.Field> fields = schema.getFields();
        System.out.println(schema);
        Assert.assertEquals(fields.get(0).name(), InterfaceName.Id.toString());
        Assert.assertEquals(fields.get(1).name(), ScoreResultField.ModelId.displayName);
        Assert.assertEquals(fields.get(2).name(), ScoreResultField.Percentile.displayName);
        Assert.assertEquals(fields.get(3).name(), ScoreResultField.Rating.displayName);
        Assert.assertEquals(fields.get(4).name(), ScoreResultField.RawScore.displayName);
        Assert.assertEquals(fields.size(), 8);

        List<String> csvfiles = HdfsUtils.getFilesForDir(yarnConfiguration, targetDir, ".*.csv$");
        Assert.assertNotNull(csvfiles);
        Assert.assertEquals(csvfiles.size(), 1);
    }

    private void generateDefaultBucketMetadata(ModelSummary modelSummary, CustomerSpace customerSpace) {
        List<BucketMetadata> bucketMetadataList = ScoringTestUtils.generateDefaultBucketMetadataList();
        CreateBucketMetadataRequest request = new CreateBucketMetadataRequest();
        request.setBucketMetadataList(bucketMetadataList);
        request.setModelGuid(modelSummary.getId());
        bucketedScoreProxy.createABCDBuckets(customerSpace.toString(), request);
    }

    private ModelSummary createModel(ModelSummaryProxy modelSummaryProxy, Tenant tenant,
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
        modelSummary.setModelType(ModelType.PYTHONMODEL.getModelType());
        modelSummary.setStatus(ModelSummaryStatus.INACTIVE);
        String dataCloudVersion = columnMetadataProxy
                .latestVersion(//
                        null)//
                .getVersion();
        modelSummary.setDataCloudVersion(dataCloudVersion);

        ModelSummary retrievedSummary = modelSummaryProxy.getModelSummaryFromModelId(customerSpace.toString(),
                modelConfiguration.getModelId());
        if (retrievedSummary != null) {
            modelSummaryProxy.deleteByModelId(customerSpace.toString(), modelConfiguration.getModelId());
        }
        modelSummaryProxy.createModelSummary(customerSpace.toString(), modelSummary, false);

        AttributeMap attrMap = new AttributeMap();
        attrMap.put(ModelSummary.STATUS, ModelSummaryStatus.ACTIVE.getStatusCode());
        modelSummaryProxy.update(customerSpace.toString(), modelSummary.getId(), attrMap);

        retrievedSummary = modelSummaryProxy.getModelSummaryFromModelId(customerSpace.toString(),
                modelConfiguration.getModelId());
        assertNotNull(retrievedSummary);
        return modelSummary;
    }

    private void setupHdfsArtifacts(Configuration yarnConfiguration, Tenant tenant,
            ScoringTestModelConfiguration modelConfiguration) throws IOException {
        String tenantId = tenant.getId();
        artifactTableDir = String.format(ModelRetrieverImpl.HDFS_SCORE_ARTIFACT_EVENTTABLE_DIR, tenantId,
                modelConfiguration.getEventTable());
        artifactTableDir = artifactTableDir.replaceAll("\\*", "Event");
        artifactBaseDir = String.format(ModelRetrieverImpl.HDFS_SCORE_ARTIFACT_BASE_DIR, tenantId,
                modelConfiguration.getEventTable(), modelConfiguration.getModelVersion(),
                modelConfiguration.getParsedApplicationId());
        enhancementsDir = artifactBaseDir + ModelJsonTypeHandler.HDFS_ENHANCEMENTS_DIR;
        String inputDataDir = TEST_INPUT_DATA_DIR + AVRO_FILE_SUFFIX;

        URL dataCompositionUrl = ClassLoader.getSystemResource(
                modelConfiguration.getLocalModelPath() + ModelJsonTypeHandler.DATA_COMPOSITION_FILENAME);
        URL modelJsonUrl = ClassLoader.getSystemResource(modelConfiguration.getModelSummaryJsonLocalpath());
        URL rfpmmlUrl = ClassLoader
                .getSystemResource(modelConfiguration.getLocalModelPath() + ModelJsonTypeHandler.PMML_FILENAME);
        URL scoreDerivationUrl = ClassLoader.getSystemResource(
                modelConfiguration.getLocalModelPath() + ModelJsonTypeHandler.SCORE_DERIVATION_FILENAME);
        URL inputAvroFile = ClassLoader.getSystemResource(LOCAL_DATA_DIR + AVRO_FILE);

        HdfsUtils.rmdir(yarnConfiguration, artifactTableDir);
        HdfsUtils.rmdir(yarnConfiguration, artifactBaseDir);
        HdfsUtils.rmdir(yarnConfiguration, enhancementsDir);
        HdfsUtils.rmdir(yarnConfiguration, TEST_INPUT_DATA_DIR);

        HdfsUtils.mkdir(yarnConfiguration, artifactTableDir);
        HdfsUtils.mkdir(yarnConfiguration, artifactBaseDir);
        HdfsUtils.mkdir(yarnConfiguration, enhancementsDir);
        HdfsUtils.mkdir(yarnConfiguration, inputDataDir);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, dataCompositionUrl.getFile(),
                artifactTableDir + ModelJsonTypeHandler.DATA_COMPOSITION_FILENAME);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelJsonUrl.getFile(),
                artifactBaseDir + modelConfiguration.getTestModelFolderName() + "_model.json");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, rfpmmlUrl.getFile(),
                artifactBaseDir + ModelJsonTypeHandler.PMML_FILENAME);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, scoreDerivationUrl.getFile(),
                enhancementsDir + ModelJsonTypeHandler.SCORE_DERIVATION_FILENAME);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, dataCompositionUrl.getFile(),
                enhancementsDir + ModelJsonTypeHandler.DATA_COMPOSITION_FILENAME);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, inputAvroFile.getFile(), inputDataDir);
    }

    private void saveAttributeSelection(CustomerSpace customerSpace) {
        LeadEnrichmentAttributesOperationMap selectedAttributeMap = checkSelection(customerSpace);
        plsInternalProxy.saveLeadEnrichmentAttributes(customerSpace, selectedAttributeMap);
    }

    private LeadEnrichmentAttributesOperationMap checkSelection(CustomerSpace customerSpace) {
        List<LeadEnrichmentAttribute> enrichmentAttributeList = plsInternalProxy
                .getLeadEnrichmentAttributes(customerSpace, null, null, false);
        LeadEnrichmentAttributesOperationMap selectedAttributeMap = new LeadEnrichmentAttributesOperationMap();
        List<String> selectedAttributes = new ArrayList<>();
        selectedAttributeMap.setSelectedAttributes(selectedAttributes);
        List<String> deselectedAttributes = new ArrayList<>();
        selectedAttributeMap.setDeselectedAttributes(deselectedAttributes);
        int premiumSelectCount = 2;
        int selectCount = 1;

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
