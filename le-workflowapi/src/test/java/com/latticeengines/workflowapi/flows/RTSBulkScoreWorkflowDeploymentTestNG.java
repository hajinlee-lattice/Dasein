package com.latticeengines.workflowapi.flows;

import static org.testng.Assert.assertNotNull;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema.Type;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import au.com.bytecode.opencsv.CSVReader;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttributesOperationMap;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.pls.ModelType;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.scoringapi.Model;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.leadprioritization.workflow.RTSBulkScoreWorkflow;
import com.latticeengines.leadprioritization.workflow.RTSBulkScoreWorkflowConfiguration;
import com.latticeengines.pls.workflow.RTSBulkScoreWorkflowSubmitter;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;
import com.latticeengines.testframework.domain.pls.ModelSummaryUtils;

import edu.emory.mathcs.backport.java.util.Arrays;

public class RTSBulkScoreWorkflowDeploymentTestNG extends ScoreWorkflowDeploymentTestNG {

    @Value("${scoring.pls.api.hostport}")
    private String plsApiHostPort;

    @Autowired
    private RTSBulkScoreWorkflow rtsBulkScoreWorkflow;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private RTSBulkScoreWorkflowSubmitter rtsBulkScoreWorkflowSubmitter;

    @Autowired
    private MetadataProxy metadataProxy;

    private static String TEST_INPUT_DATA_DIR;

    private static String SCORED_FILE_DIR;

    private static final String AVRO_FILE_SUFFIX = "File/SourceFile_file_1462229180545_csv/Extracts/2016-05-02-18-47-03/";

    private static final String AVRO_FILE = "part-m-00000_small.avro";

    private static final String TEST_MODEL_NAME_PREFIX = "c8684c37-a3b9-452f-b7e3-af440e4365b8";

    private static final String LOCAL_DATA_DIR = "com/latticeengines/scoring/rts/data/";

    private LeadEnrichmentAttributesOperationMap selectedAttributeMap;

    protected static String TENANT_ID;

    protected Tenant tenant;

    protected com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy internalResourceRestApiProxy;

    protected static CustomerSpace customerSpace;

    private String artifactTableDir;
    private String artifactBaseDir;
    private String enhancementsDir;

    private ModelSummary summary;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        TENANT_ID = this.getClass().getSimpleName() + String.valueOf(System.currentTimeMillis());
        customerSpace = CustomerSpace.parse(TENANT_ID);
        TEST_INPUT_DATA_DIR = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId().toString(), customerSpace)
                .toString();
        SCORED_FILE_DIR = PathBuilder.buildDataFilePath(CamilleEnvironment.getPodId().toString(), customerSpace)
                .toString() + "/Exports";
        System.out.println("SCORED_FILE_DIR is " + SCORED_FILE_DIR);

        String testModelFolderName = TEST_MODEL_NAME_PREFIX;
        String applicationId = "application_" + "1457046993615_3823";
        String modelVersion = "157342cb-a8fb-4158-b62a-699441401e9a";
        ScoringTestModelConfiguration modelConfiguration = new ScoringTestModelConfiguration(testModelFolderName,
                applicationId, modelVersion);
        tenant = setupTenant();
        summary = createModel(tenant, modelConfiguration, customerSpace);
        setupHdfsArtifacts(yarnConfiguration, tenant, modelConfiguration);
        saveAttributeSelection(customerSpace);
    }

    private void saveAttributeSelection(CustomerSpace customerSpace) {
        internalResourceRestApiProxy = new com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy(
                plsApiHostPort);
        selectedAttributeMap = checkSelection(customerSpace);
        System.out.println(selectedAttributeMap.getDeselectedAttributes());
        System.out.println(selectedAttributeMap.getSelectedAttributes());
        internalResourceRestApiProxy.saveLeadEnrichmentAttributes(customerSpace, selectedAttributeMap);
    }

    private LeadEnrichmentAttributesOperationMap checkSelection(CustomerSpace customerSpace) {
        List<LeadEnrichmentAttribute> enrichmentAttributeList = internalResourceRestApiProxy
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

    @AfterClass(groups = "deployment")
    public void cleanup() throws IOException {
        internalResourceProxy.deleteTenant(customerSpace);
        HdfsUtils.rmdir(yarnConfiguration, artifactTableDir);
        HdfsUtils.rmdir(yarnConfiguration, artifactBaseDir);
        HdfsUtils.rmdir(yarnConfiguration, enhancementsDir);
        HdfsUtils.rmdir(yarnConfiguration, TEST_INPUT_DATA_DIR);
        HdfsUtils.rmdir(yarnConfiguration, SCORED_FILE_DIR);
    }

    @Test(groups = "deployment", enabled = true)
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
        try (CSVReader reader = new CSVReader(new InputStreamReader(HdfsUtils.getInputStream(yarnConfiguration,
                files.get(0))))) {
            String[] header = reader.readNext();
            System.out.println(Arrays.toString(header));
            Assert.assertEquals(header[header.length - 4], "Score");
            Assert.assertTrue(headerBelongsToLeadEnrichmentAttributes(header[header.length - 1]));
            Assert.assertTrue(headerBelongsToLeadEnrichmentAttributes(header[header.length - 2]));
            Assert.assertTrue(headerBelongsToLeadEnrichmentAttributes(header[header.length - 3]));
        }
    }

    private boolean headerBelongsToLeadEnrichmentAttributes(String header) {
        return selectedAttributeMap.getSelectedAttributes().contains(header);
    }

    private void score(String modelId, String tableToScore) throws Exception {
        RTSBulkScoreWorkflowConfiguration rtsBulkScoreWorkflowConfig = rtsBulkScoreWorkflowSubmitter
                .generateConfiguration(modelId, tableToScore, tableToScore, true, false);
        WorkflowExecutionId workflowId = workflowService.start(rtsBulkScoreWorkflow.name(), rtsBulkScoreWorkflowConfig);
        waitForCompletion(workflowId);
    }

    private Tenant setupTenant() throws Exception {
        Tenant returnTenant = setupTenant(customerSpace);
        setupUsers(customerSpace);
        MultiTenantContext.setTenant(returnTenant);
        assertNotNull(MultiTenantContext.getTenant());
        return returnTenant;
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
        Attribute idAttr = new Attribute();
        idAttr.setName(InterfaceName.Id.name());
        idAttr.setDisplayName(InterfaceName.Id.name());
        idAttr.setSourceLogicalDataType("");
        idAttr.setPhysicalDataType(Type.STRING.name());
        metadataTable.addAttribute(idAttr);
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

        ModelSummary retrievedSummary = internalResourceProxy.getModelSummaryFromModelId(
                modelConfiguration.getModelId(), customerSpace);
        if (retrievedSummary != null) {
            internalResourceProxy.deleteModelSummary(modelConfiguration.getModelId(), customerSpace);
        }
        internalResourceProxy.createModelSummary(modelSummary, customerSpace);
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

        URL dataCompositionUrl = ClassLoader.getSystemResource(modelConfiguration.getLocalModelPath()
                + Model.DATA_COMPOSITION_FILENAME);
        URL modelJsonUrl = ClassLoader.getSystemResource(modelConfiguration.getModelSummaryJsonLocalpath());
        URL rfpmmlUrl = ClassLoader.getSystemResource(modelConfiguration.getLocalModelPath() + Model.PMML_FILENAME);
        URL scoreDerivationUrl = ClassLoader.getSystemResource(modelConfiguration.getLocalModelPath()
                + Model.SCORE_DERIVATION_FILENAME);
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
                + Model.DATA_COMPOSITION_FILENAME);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelJsonUrl.getFile(),
                artifactBaseDir + modelConfiguration.getTestModelFolderName() + "_model.json");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, rfpmmlUrl.getFile(), artifactBaseDir + Model.PMML_FILENAME);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, scoreDerivationUrl.getFile(), enhancementsDir
                + Model.SCORE_DERIVATION_FILENAME);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, dataCompositionUrl.getFile(), enhancementsDir
                + Model.DATA_COMPOSITION_FILENAME);
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
            this.modelSummaryJsonLocalpath = localModelPath + Model.MODEL_JSON;
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
