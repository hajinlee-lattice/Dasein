package com.latticeengines.scoring.runtime.mapreduce;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFilenameFilter;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.dataplatform.exposed.service.ModelingService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modeling.Algorithm;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.ModelDefinition;
import com.latticeengines.domain.exposed.modeling.algorithm.RandomForestAlgorithm;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStatus;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStep;
import com.latticeengines.scoring.entitymanager.ScoringCommandEntityMgr;
import com.latticeengines.scoring.functionalframework.ScoringFunctionalTestNGBase;
import com.latticeengines.scoring.orchestration.service.ScoringDaemonService;
import com.latticeengines.scoring.orchestration.service.ScoringStepYarnProcessor;
public class ScoringComparisonAgainstModelingTestNG extends ScoringFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ScoringComparisonAgainstModelingTestNG.class);

    private static final double EPS = 1e-6;

    @Value("${dataplatform.customer.basedir}")
    protected String customerBaseDir;

    @Inject
    protected Configuration yarnConfiguration;

    protected static String customer = "Mulesoft_Relaunch_Orchestration";

    protected static String tenant;

    @Inject
    protected ModelingService modelingService;

    private String inputLeadsTable;

    protected Model model;

    protected String modelingModelPath;

    protected String uuid;

    protected String containerId;

    protected String path;

    protected String dataPath;

    protected String samplePath;

    protected String metadataPath;

    protected String scoringDataPath;

    protected String scorePath;

    @Inject
    private ScoringCommandEntityMgr scoringCommandEntityMgr;

    @Inject
    private ScoringStepYarnProcessor scoringStepYarnProcessor;

    @Inject
    private JdbcTemplate scoringJdbcTemplate;

    @Value("${scoring.test.table}")
    private String testInputTable;

    @BeforeMethod(groups = "sqoop")
    public void beforeMethod() throws Exception {
    }

    @BeforeClass(groups = "sqoop, enabled = false")
    public void setup() throws Exception {
        tenant = CustomerSpace.parse(customer).toString();
        path = customerBaseDir + "/" + tenant;
        dataPath = customerBaseDir + "/" + tenant + "/data/Q_PLS_ModelingMulesoft_Relaunch/";
        samplePath = customerBaseDir + "/" + tenant + "/data/Q_PLS_ModelingMulesoft_Relaunch/samples/";
        metadataPath = customerBaseDir + "/" + tenant + "/data/EventMetadata/";
        modelingModelPath = customerBaseDir + "/" + tenant + "/models/Q_PLS_ModelingMulesoft_Relaunch/";
        inputLeadsTable = getClass().getSimpleName() + "_LeadsTable";
        scorePath = customerBaseDir + "/" + tenant + "/scoring/" + inputLeadsTable + "/scores";
        if (!CollectionUtils.isEmpty(dbMetadataService.showTable(scoringJdbcTemplate, inputLeadsTable))) {
            dbMetadataService.dropTable(scoringJdbcTemplate, inputLeadsTable);
        }
        dbMetadataService.createNewTableFromExistingOne(scoringJdbcTemplate, inputLeadsTable, testInputTable);
    }

    @Test(groups = "sqoop", enabled = false)
    public void modelScoreAndCompare() throws Exception {
        prepareDataForModeling();
        model();
        prepareDataForScoring();
        score();
        assertTrue(compareEvaluationResults());
    }

    // upload necessary files to the directory
    protected void prepareDataForModeling() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, path);
        HdfsUtils.mkdir(yarnConfiguration, samplePath);
        HdfsUtils.mkdir(yarnConfiguration, metadataPath);

        String schemaPath = dataPath + "schema-mulesoft.avsc";
        File schemaFile = new File("../le-dataplatform/src/test/python/data/schema-mulesoft.avsc");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, schemaFile.getAbsolutePath(), schemaPath);

        String testDataPath = samplePath + "s100Test-mulesoft.avro";
        File testDataFile = new File("../le-dataplatform/src/test/python/data/s100Test-mulesoft.avro");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, testDataFile.getAbsolutePath(), testDataPath);

        HdfsUtils.copyLocalToHdfs(yarnConfiguration, testDataFile.getAbsolutePath(), dataPath);

        String trainingDataPath = samplePath + "s100Training-mulesoft.avro";
        File trainingDataFile = new File("../le-dataplatform/src/test/python/data/s100Training-mulesoft.avro");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, trainingDataFile.getAbsolutePath(), trainingDataPath);

        String metadataMulesoftPath = metadataPath + "metadata-mulesoft.avsc";
        File metadataMulesoftFile = new File("../le-dataplatform/src/test/python/data/metadata-mulesoft.avsc");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, metadataMulesoftFile.getAbsolutePath(), metadataMulesoftPath);

        // make up the contents of diagnostics.json to let it pass the
        // validation step
        ObjectNode summaryObj = new ObjectMapper().createObjectNode();
        ObjectNode sampleSizeObj = new ObjectMapper().createObjectNode();
        sampleSizeObj.put("SampleSize", 20000);
        summaryObj.set("Summary", sampleSizeObj);
        String diagnosticFilePath = metadataPath + "diagnostics.json";
        HdfsUtils.writeToFile(yarnConfiguration, diagnosticFilePath, summaryObj.toString());

        String profileMulesoftScoringPath = metadataPath + "profile.avro";
        File profileMulesoftScoringFile = new File(
                "../le-dataplatform/src/test/python/data/profile-mulesoft-scoring.avro");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, profileMulesoftScoringFile.getAbsolutePath(),
                profileMulesoftScoringPath);
    }

    protected void model() throws Exception {
        RandomForestAlgorithm randomForestAlgorithm = new RandomForestAlgorithm();
        randomForestAlgorithm.setPriority(0);
        randomForestAlgorithm.setContainerProperties("VIRTUALCORES=1 MEMORY=2048 PRIORITY=0");
        randomForestAlgorithm.setSampleName("s100");

        ModelDefinition modelDef = new ModelDefinition();
        modelDef.setName("Model1");
        modelDef.addAlgorithms(Arrays.<Algorithm> asList(new Algorithm[] { randomForestAlgorithm }));

        model = createModel(modelDef);
        submitModel();
    }

    protected void prepareDataForScoring() throws Exception {
        uuid = getUuid();
        containerId = getContainerId();
        System.out.println("uuid is " + uuid);
        String modelId = "ms__" + uuid + "-PLS_model";

        File scoringLeadFile = addColumnsToTestDataFile(modelId);
        scoringJdbcTemplate.execute(String.format("Update [%s] Set [%s] = '%s'", inputLeadsTable,
                ScoringDaemonService.MODEL_GUID, modelId));
        scoringDataPath = customerBaseDir + "/" + tenant + "/scoring/" + inputLeadsTable + "/data/1.avro";
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, scoringLeadFile.getAbsolutePath(), scoringDataPath);
        // delete the temp file
        scoringLeadFile.delete();
    }

    protected String getUuid() throws Exception {
        List<String> dirList = HdfsUtils.getFilesForDir(yarnConfiguration, modelingModelPath);
        assertTrue(dirList.size() == 1, "There should be only one model generated.");
        return UuidUtils.parseUuid(dirList.get(0));
    }

    protected String getContainerId() throws Exception {
        List<String> topDirs;
        topDirs = HdfsUtils.getFilesForDir(yarnConfiguration, modelingModelPath + uuid);
        String dir = topDirs.get(0);
        return dir.substring(dir.lastIndexOf("/") + 1);
    }

    private File addColumnsToTestDataFile(String modelId) throws IllegalArgumentException, Exception {
        String testDataPath = samplePath + "s100Test-mulesoft.avro";
        List<GenericRecord> records = AvroUtils.getData(yarnConfiguration, new Path(testDataPath));

        // schema.avsc is the schema combined the leadID and Model_GUID columns
        URL url = ClassLoader.getSystemResource("com/latticeengines/scoring/avro/schema.avsc");
        Schema schema = new Schema.Parser().parse(new File(url.getFile()));
        File outputFile = File.createTempFile("inputLeads", ".avro");
        DatumWriter<GenericRecord> userDatumWriter = new SpecificDatumWriter<GenericRecord>();
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(userDatumWriter);

        for (int i = 0; i < records.size(); i++) {
            GenericRecord result = records.get(i);
            if (i == 0) {
                dataFileWriter.create(schema, outputFile);
            }
            GenericRecord record = new GenericData.Record(schema);
            int modelingId = (int) result.get("ModelingID");
            record.put("LeadID", String.valueOf(modelingId));
            record.put("Model_GUID", modelId);
            for (int j = 0; j < result.getSchema().getFields().size(); j++) {
                String key = result.getSchema().getFields().get(j).name();
                record.put(key, result.get(key));
            }
            dataFileWriter.append(record);
        }
        dataFileWriter.close();
        return outputFile;
    }

    private Model createModel(ModelDefinition modelDef) {
        Model m = new Model();
        m.setModelDefinition(modelDef);
        m.setName("Model Submission1");
        m.setTable("Q_PLS_ModelingMulesoft_Relaunch");
        m.setMetadataTable("EventMetadata");
        m.setTargetsList(Arrays.<String> asList(new String[] { "P1_Event" }));
        m.setKeyCols(Arrays.<String> asList(new String[] { "ModelingID" }));
        m.setCustomer(getCustomer());
        m.setDataFormat("avro");

        return m;
    }

    public String getCustomer() {
        return tenant;
    }

    protected void submitModel() throws Exception {
        List<String> features = modelingService.getFeatures(model, false);
        model.setFeaturesList(features);

        List<ApplicationId> appIds = modelingService.submitModel(model);

        for (ApplicationId appId : appIds) {
            FinalApplicationStatus status = waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
            assertEquals(status, FinalApplicationStatus.SUCCEEDED);

            JobStatus jobStatus = modelingService.getJobStatus(appId.toString());
            String modelFile = HdfsUtils.getFilesForDir(yarnConfiguration, jobStatus.getResultDirectory()).get(0);
            String modelContents = HdfsUtils.getHdfsFileContents(yarnConfiguration, modelFile);
            assertNotNull(modelContents);
        }
    }

    protected void score() throws Exception {
        ScoringCommand scoringCommand = new ScoringCommand(customer, ScoringCommandStatus.POPULATED, inputLeadsTable, 0,
                4352, new Timestamp(System.currentTimeMillis()));
        scoringCommandEntityMgr.create(scoringCommand);
        ApplicationId appId = scoringStepYarnProcessor.executeYarnStep(scoringCommand, ScoringCommandStep.SCORE_DATA);
        waitForSuccess(appId, ScoringCommandStep.SCORE_DATA);
    }

    private boolean compareEvaluationResults() throws Exception {
        boolean resultsAreSame = false;
        // locate the scored.txt after modeling is done
        Map<String, Double> modelingResults = getModelingResults();
        // locate the avro file after scoring is done
        Map<String, Double> scoringResults = getScoringResults();
        // compare the results
        resultsAreSame = compareModelingAndScoringResults(modelingResults, scoringResults);
        return resultsAreSame;
    }

    private Map<String, Double> getModelingResults() throws Exception {

        String modelingResultsPath = modelingModelPath + uuid + "/" + containerId;
        System.out.println("modelingResultsPath is " + modelingResultsPath);

        HdfsFilenameFilter filter = new HdfsFilenameFilter() {
            @Override
            public boolean accept(String path) {
                return path.endsWith("_scored.txt");
            }
        };
        List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, modelingResultsPath, filter);
        if (files.size() != 1) {
            throw new FileNotFoundException("modelingResult is not found.");
        }

        Map<String, Double> modelingResults = new HashMap<String, Double>();
        String resultString = HdfsUtils.getHdfsFileContents(yarnConfiguration, files.get(0));
        String[] rows = resultString.split("\n");
        for (String row : rows) {
            String[] columns = row.split(",");
            assert (columns.length == 2);
            String leadId = columns[0];
            Double score = Double.valueOf(columns[1]);
            modelingResults.put(leadId, score);
        }
        return modelingResults;
    }

    private Map<String, Double> getScoringResults() {
        List<GenericRecord> resultList = loadHDFSAvroFiles(yarnConfiguration, scorePath);
        Map<String, Double> scoringResults = new HashMap<String, Double>();
        for (GenericRecord result : resultList) {
            String leadId = String.valueOf(result.get("LeadID"));
            Double score = (Double) result.get("RawScore");
            scoringResults.put(leadId, score);
        }
        return scoringResults;
    }

    private ArrayList<GenericRecord> loadHDFSAvroFiles(Configuration configuration, String hdfsDir) {
        ArrayList<GenericRecord> newlist = new ArrayList<GenericRecord>();
        List<String> files = null;

        HdfsFilenameFilter filter = new HdfsFilenameFilter() {
            @Override
            public boolean accept(String path) {
                return path.endsWith(".avro");
            }
        };
        try {
            files = HdfsUtils.getFilesForDir(configuration, hdfsDir, filter);
        } catch (Exception e) {
            log.error("Failed to get files.", e);
        }
        if (files.size() == 0) {
            throw new LedpException(LedpCode.LEDP_15003, new String[] { "avro" });
        }
        for (String file : files) {
            try {
                List<GenericRecord> list = AvroUtils.getData(configuration, new Path(file));
                newlist.addAll(list);
            } catch (Exception e) {
                log.error("Failed to get avro data.", e);
            }
        }
        return newlist;
    }

    private boolean compareModelingAndScoringResults(Map<String, Double> modelingResults,
            Map<String, Double> scoringResults) {
        if (modelingResults.size() != scoringResults.size()) {
            System.err.println("the size of the results is not the same");
            return false;
        }
        for (String leadId : modelingResults.keySet()) {
            String leadIdWithoutZeros = leadId;
            Double modelingResult = modelingResults.get(leadId);
            // get rid of the zeros after the digits since modeling makes leadId
            // double
            if (leadId.contains(".")) {
                leadIdWithoutZeros = leadId.substring(0, leadId.indexOf("."));
                assertTrue(Double.parseDouble(leadIdWithoutZeros) - Double.parseDouble(leadId) < EPS,
                        "The leadIdWithoutZeros should be the same as leadId");
            }
            if (!scoringResults.containsKey(leadIdWithoutZeros)) {
                System.err.println("In scoringResults, the leadId: " + leadIdWithoutZeros + " is missing.");
                return false;
            } else {
                Double scoringResult = scoringResults.get(leadIdWithoutZeros);
                if (modelingResult.compareTo(scoringResult) != 0) {
                    System.err.println("For " + leadIdWithoutZeros + ", the scoringResult: " + scoringResult
                            + " does not match with that of " + modelingResult + " in modeling");
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    @AfterMethod(enabled = false, lastTimeOnly = true, alwaysRun = false)
    public void afterEachTest() {
        try {
            dbMetadataService.dropTable(scoringJdbcTemplate, inputLeadsTable);
            HdfsUtils.rmdir(yarnConfiguration, path);
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

}
