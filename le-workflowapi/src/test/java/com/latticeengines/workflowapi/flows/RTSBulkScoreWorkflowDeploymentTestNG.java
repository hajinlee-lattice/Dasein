package com.latticeengines.workflowapi.flows;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
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
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketName;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttributesOperationMap;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.pls.ModelType;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.scoringapi.Model;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.lp.CreateBucketMetadataRequest;
import com.latticeengines.domain.exposed.serviceflows.scoring.RTSBulkScoreWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.pls.workflow.RTSBulkScoreWorkflowSubmitter;
import com.latticeengines.proxy.exposed.lp.BucketedScoreProxy;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.testframework.exposed.utils.ModelSummaryUtils;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

import au.com.bytecode.opencsv.CSVReader;

public class RTSBulkScoreWorkflowDeploymentTestNG extends ScoreWorkflowDeploymentTestNGBase {

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private RTSBulkScoreWorkflowSubmitter rtsBulkScoreWorkflowSubmitter;

    @Autowired
    private MetadataProxy metadataProxy;

    @Inject
    private BucketedScoreProxy bucketedScoreProxy;

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    private static String TEST_INPUT_DATA_DIR;

    private static String SCORED_FILE_DIR;

    private static final String AVRO_FILE_SUFFIX = "File/SourceFile_file_1462229180545_csv/Extracts/2016-05-02-18-47-03/";

    private static final String AVRO_FILE = "part-m-00000_small.avro";

    private static final String TEST_MODEL_NAME_PREFIX = "c8684c37-a3b9-452f-b7e3-af440e4365b8";

    private static final String LOCAL_DATA_DIR = "com/latticeengines/scoring/rts/data/";

    private List<String> selectedAttributeNameList;

    private String artifactTableDir;
    private String artifactBaseDir;
    private String enhancementsDir;

    private ModelSummary summary;

    @Override
    @BeforeClass(groups = "workflow")
    public void setup() throws Exception {
        setupTestEnvironment(LatticeProduct.LPA3);
        TEST_INPUT_DATA_DIR = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), mainTestCustomerSpace)
                .toString();
        SCORED_FILE_DIR = PathBuilder.buildDataFilePath(CamilleEnvironment.getPodId(), mainTestCustomerSpace).toString()
                + "/Exports";
        System.out.println("SCORED_FILE_DIR is " + SCORED_FILE_DIR);

        String applicationId = "application_1457046993615_3823";
        String modelVersion = "157342cb-a8fb-4158-b62a-699441401e9a";
        String uuid = UUID.randomUUID().toString();
        ScoringTestModelConfiguration modelConfiguration = new ScoringTestModelConfiguration(TEST_MODEL_NAME_PREFIX,
                applicationId, modelVersion, uuid);
        summary = createModel(mainTestTenant, modelConfiguration, mainTestCustomerSpace);
        generateDefaultBucketMetadata(summary, mainTestCustomerSpace);
        setupHdfsArtifacts(yarnConfiguration, mainTestTenant, modelConfiguration);
        saveAttributeSelection(mainTestCustomerSpace);
    }

    private void generateDefaultBucketMetadata(ModelSummary summary, CustomerSpace customerSpace) {
        List<BucketMetadata> bucketMetadataList = generateDefaultBucketMetadataList();
        CreateBucketMetadataRequest request = new CreateBucketMetadataRequest();
        request.setBucketMetadataList(bucketMetadataList);
        request.setModelGuid(summary.getId());
        bucketedScoreProxy.createABCDBuckets(customerSpace.toString(), request);
    }

    private void saveAttributeSelection(CustomerSpace customerSpace) {

        LeadEnrichmentAttributesOperationMap selectedAttributeMap = checkSelection(customerSpace);
        System.out.println("The deselected attributes are: " + selectedAttributeMap.getDeselectedAttributes());
        System.out.println("The selected attributes are: " + selectedAttributeNameList);
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
        selectedAttributeNameList = new ArrayList<>();
        for (LeadEnrichmentAttribute attr : enrichmentAttributeList) {
            if (attr.getIsPremium()) {
                if (premiumSelectCount > 0) {
                    premiumSelectCount--;
                    selectedAttributes.add(attr.getFieldName());
                    selectedAttributeNameList.add(attr.getDisplayName());
                }
            } else {
                if (selectCount > 0) {
                    selectCount--;
                    selectedAttributes.add(attr.getFieldName());
                    selectedAttributeNameList.add(attr.getDisplayName());
                }
            }
        }

        return selectedAttributeMap;
    }

    @AfterClass(groups = "workflow")
    public void cleanup() throws IOException {
        HdfsUtils.rmdir(yarnConfiguration, artifactTableDir);
        HdfsUtils.rmdir(yarnConfiguration, artifactBaseDir);
        HdfsUtils.rmdir(yarnConfiguration, enhancementsDir);
        HdfsUtils.rmdir(yarnConfiguration, TEST_INPUT_DATA_DIR);
        HdfsUtils.rmdir(yarnConfiguration, SCORED_FILE_DIR);
    }

    @Test(groups = "workflow")
    public void testScoreAccount() throws Exception {
        Assert.assertNotNull(summary);
        score(summary.getId(), summary.getTrainingTableName());
        testCsvFile();
    }

    private void testCsvFile() throws IOException {
        // find the csv file
        List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, SCORED_FILE_DIR);
        Assert.assertEquals(files.size(), 1);
        Assert.assertNotNull(HdfsUtils.getHdfsFileContents(yarnConfiguration, files.get(0)));
        // assert the ordering of the header
        try (CSVReader reader = new CSVReader(
                new InputStreamReader(HdfsUtils.getInputStream(yarnConfiguration, files.get(0))))) {
            String[] header = reader.readNext();
            System.out.println("The header is " + Arrays.toString(header));
            // Uncommont the following lines when PLS-3093 is resolved.
            // Assert.assertEquals(header[header.length - 5],
            // ScoreResultField.Percentile.displayName);
            // Assert.assertEquals(header[header.length - 4],
            // ScoreResultField.Rating.displayName);
            Assert.assertTrue(headerBelongsToLeadEnrichmentAttributes(header[header.length - 1]));
            Assert.assertTrue(headerBelongsToLeadEnrichmentAttributes(header[header.length - 2]));
            Assert.assertTrue(headerBelongsToLeadEnrichmentAttributes(header[header.length - 3]));
        }
    }

    private boolean headerBelongsToLeadEnrichmentAttributes(String header) {
        return selectedAttributeNameList.contains(header);
    }

    private void score(String modelId, String tableToScore) throws Exception {
        RTSBulkScoreWorkflowConfiguration workflowConfig = rtsBulkScoreWorkflowSubmitter.generateConfiguration(modelId,
                tableToScore, tableToScore, true, false);

        workflowService.registerJob(workflowConfig, applicationContext);
        WorkflowExecutionId workflowId = workflowService.start(workflowConfig);
        waitForCompletion(workflowId);
    }

    private ModelSummary createModel(Tenant tenant, ScoringTestModelConfiguration modelConfiguration,
            CustomerSpace customerSpace) throws IOException {
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

        Table metadataTable = new Table();
        Extract extract = new Extract();
        extract.setName("ExtractTable");
        extract.setProcessedRecords(100L);
        extract.setTenantId(tenant.getPid());
        extract.setPath(TEST_INPUT_DATA_DIR + AVRO_FILE_SUFFIX + AVRO_FILE);
        extract.setExtractionTimestamp(12345L);
        extract.setTable(metadataTable);

        URL inputAvroFile = ClassLoader.getSystemResource(LOCAL_DATA_DIR + AVRO_FILE);
        Schema inputAvroSchema = AvroUtils.readSchemaFromLocalFile(inputAvroFile.getPath());
        for (Schema.Field field : inputAvroSchema.getFields()) {
            Attribute attribute = new Attribute();
            attribute.setName(field.name());
            attribute.setDisplayName(field.name());
            attribute.setSourceLogicalDataType("");
            attribute.setPhysicalDataType(Type.STRING.name());
            metadataTable.addAttribute(attribute);
        }

        metadataTable.setName("MetadataTable");
        metadataTable.setTableType(TableType.DATATABLE);
        metadataTable.addExtract(extract);
        metadataTable.setDisplayName("MetadataTable");
        metadataTable.setInterpretation(SchemaInterpretation.SalesforceAccount.name());
        metadataTable.setTenant(tenant);
        metadataTable.setTenantId(tenant.getPid());
        metadataTable.setMarkedForPurge(true);
        metadataProxy.createTable(customerSpace.toString(), "MetadataTable", metadataTable);

        modelSummary.setTrainingTableName(metadataTable.getName());

        ModelSummary retrievedSummary = modelSummaryProxy
                .getModelSummaryFromModelId(tenant.getId(), modelConfiguration.getModelId());
        if (retrievedSummary != null) {
            modelSummaryProxy.deleteByModelId(customerSpace.toString(), modelConfiguration.getModelId());
        }
        modelSummaryProxy.createModelSummary(customerSpace.toString(), modelSummary, false);
        return modelSummary;
    }

    private void setupHdfsArtifacts(Configuration yarnConfiguration, Tenant tenant,
            ScoringTestModelConfiguration modelConfiguration) throws IOException {
        String tenantId = tenant.getId();
        artifactTableDir = String.format(Model.HDFS_SCORE_ARTIFACT_EVENTTABLE_DIR, tenantId,
                modelConfiguration.getEventTable());
        artifactBaseDir = String.format(Model.HDFS_SCORE_ARTIFACT_BASE_DIR, tenantId,
                modelConfiguration.getEventTable(), modelConfiguration.getModelVersion(),
                modelConfiguration.getParsedApplicationId());
        enhancementsDir = artifactBaseDir + Model.HDFS_ENHANCEMENTS_DIR;
        String inputDataDir = TEST_INPUT_DATA_DIR + AVRO_FILE_SUFFIX;

        URL dataCompositionUrl = ClassLoader
                .getSystemResource(modelConfiguration.getLocalModelPath() + Model.DATA_COMPOSITION_FILENAME);
        URL modelJsonUrl = ClassLoader.getSystemResource(modelConfiguration.getModelSummaryJsonLocalpath());
        URL rfpmmlUrl = ClassLoader.getSystemResource(modelConfiguration.getLocalModelPath() + Model.PMML_FILENAME);
        URL scoreDerivationUrl = ClassLoader
                .getSystemResource(modelConfiguration.getLocalModelPath() + Model.SCORE_DERIVATION_FILENAME);
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
                artifactTableDir + Model.DATA_COMPOSITION_FILENAME);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelJsonUrl.getFile(),
                artifactBaseDir + modelConfiguration.getTestModelFolderName() + "_model.json");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, rfpmmlUrl.getFile(), artifactBaseDir + Model.PMML_FILENAME);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, scoreDerivationUrl.getFile(),
                enhancementsDir + Model.SCORE_DERIVATION_FILENAME);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, dataCompositionUrl.getFile(),
                enhancementsDir + Model.DATA_COMPOSITION_FILENAME);
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

        ScoringTestModelConfiguration(String testModelFolderName, String applicationId, String modelVersion,
                String uuid) {
            this.testModelFolderName = testModelFolderName;
            this.modelId = TestFrameworkUtils.MODEL_PREFIX + uuid + "_";
            this.modelName = testModelFolderName;
            this.applicationId = applicationId;
            this.parsedApplicationId = applicationId.substring(applicationId.indexOf("_") + 1);
            this.modelVersion = modelVersion;
            this.eventTable = testModelFolderName;
            this.sourceInterpretation = SchemaInterpretation.SalesforceAccount.name();
            this.modelSummaryJsonLocalpath = localModelPath + Model.MODEL_JSON;
        }

        String getTestModelFolderName() {
            return testModelFolderName;
        }

        String getModelId() {
            return modelId;
        }

        String getModelName() {
            return modelName;
        }

        String getLocalModelPath() {
            return localModelPath;
        }

        String getApplicationId() {
            return applicationId;
        }

        String getParsedApplicationId() {
            return parsedApplicationId;
        }

        String getModelVersion() {
            return modelVersion;
        }

        String getEventTable() {
            return eventTable;
        }

        String getSourceInterpretation() {
            return sourceInterpretation;
        }

        String getModelSummaryJsonLocalpath() {
            return modelSummaryJsonLocalpath;
        }
    }

    private static final Double LIFT_1 = 3.4;
    private static final Double LIFT_2 = 2.4;
    private static final Double LIFT_3 = 1.2;
    private static final Double LIFT_4 = 0.4;

    private static final int NUM_LEADS_BUCKET_1 = 28588;
    private static final int NUM_LEADS_BUCKET_2 = 14534;
    private static final int NUM_LEADS_BUCKET_3 = 25206;
    private static final int NUM_LEADS_BUCKET_4 = 25565;

    private static List<BucketMetadata> generateDefaultBucketMetadataList() {
        List<BucketMetadata> bucketMetadataList = new ArrayList<BucketMetadata>();
        BucketMetadata BUCKET_METADATA_A = new BucketMetadata();
        BucketMetadata BUCKET_METADATA_B = new BucketMetadata();
        BucketMetadata BUCKET_METADATA_C = new BucketMetadata();
        BucketMetadata BUCKET_METADATA_D = new BucketMetadata();
        bucketMetadataList.add(BUCKET_METADATA_A);
        bucketMetadataList.add(BUCKET_METADATA_B);
        bucketMetadataList.add(BUCKET_METADATA_C);
        bucketMetadataList.add(BUCKET_METADATA_D);
        BUCKET_METADATA_A.setBucket(BucketName.A);
        BUCKET_METADATA_A.setNumLeads(NUM_LEADS_BUCKET_1);
        BUCKET_METADATA_A.setLeftBoundScore(99);
        BUCKET_METADATA_A.setRightBoundScore(95);
        BUCKET_METADATA_A.setLift(LIFT_1);
        BUCKET_METADATA_B.setBucket(BucketName.B);
        BUCKET_METADATA_B.setNumLeads(NUM_LEADS_BUCKET_2);
        BUCKET_METADATA_B.setLeftBoundScore(94);
        BUCKET_METADATA_B.setRightBoundScore(85);
        BUCKET_METADATA_B.setLift(LIFT_2);
        BUCKET_METADATA_C.setBucket(BucketName.C);
        BUCKET_METADATA_C.setNumLeads(NUM_LEADS_BUCKET_3);
        BUCKET_METADATA_C.setLeftBoundScore(84);
        BUCKET_METADATA_C.setRightBoundScore(50);
        BUCKET_METADATA_C.setLift(LIFT_3);
        BUCKET_METADATA_D.setBucket(BucketName.D);
        BUCKET_METADATA_D.setNumLeads(NUM_LEADS_BUCKET_4);
        BUCKET_METADATA_D.setLeftBoundScore(49);
        BUCKET_METADATA_D.setRightBoundScore(5);
        BUCKET_METADATA_D.setLift(LIFT_4);
        return bucketMetadataList;
    }
}
