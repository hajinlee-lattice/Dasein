package com.latticeengines.scoring.service.impl;



import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
import com.latticeengines.domain.exposed.scoring.ScoringCommandStep;
import com.latticeengines.domain.exposed.scoring.ScoringConfiguration;
import com.latticeengines.scoring.functionalframework.ScoringFunctionalTestNGBase;
import com.latticeengines.scoring.service.ScoringJobService;

    public class ScoringJobServiceImplTestNG extends ScoringFunctionalTestNGBase {

        private static final double EPS = 1e-6;

        @Value("${dataplatform.customer.basedir}")
        private String customerBaseDir;

        @Autowired
        private Configuration yarnConfiguration;

        private static final String customer = "Mulesoft_Relaunch";

        private static String tenant;

        @Autowired
        private ModelingService modelingService;

        @Autowired
        private ScoringJobService scoringJobService;

        private Model model;

        private String modelingModelPath;

        private String uuid;

        private String containerId;

        private String path;

        private String dataPath;

        private String samplePath;

        private String metadataPath;

        private String scorePath;

        @BeforeMethod(groups = "functional")
        public void beforeMethod() throws Exception {
        }

        @BeforeClass(groups = "functional")
        public void setup() throws Exception {
            tenant = CustomerSpace.parse(customer).toString();
            path = customerBaseDir + "/" + tenant;
            dataPath = customerBaseDir + "/" + tenant + "/data/Q_PLS_ModelingMulesoft_Relaunch/";
            samplePath = customerBaseDir + "/" + tenant + "/data/Q_PLS_ModelingMulesoft_Relaunch/samples/";
            metadataPath = customerBaseDir + "/" + tenant + "/data/EventMetadata/";
            modelingModelPath = customerBaseDir + "/" + tenant + "/models/Q_PLS_ModelingMulesoft_Relaunch/";
            scorePath = customerBaseDir + "/" + tenant + "/scoring/someid/scores";
        }

        @Test(groups = "functional")
        public void modelScoreAndCompare() throws Exception {
            prepareDataForModeling();
            modeling();
            prepareDataForScoring();
            scoring();
            assertTrue(compareEvaluationResults());
        }

        // upload necessary files to the directory
        private void prepareDataForModeling() throws Exception {
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
            ObjectNode sampleSizeObj = summaryObj.putObject("Summary");
            sampleSizeObj.put("SampleSize", 20000);
            String diagnosticFilePath = metadataPath + "diagnostics.json";
            HdfsUtils.writeToFile(yarnConfiguration, diagnosticFilePath, summaryObj.toString());

            String profileMulesoftScoringPath = metadataPath + "profile-mulesoft-scoring.avro";
            File profileMulesoftScoringFile = new File(
                    "../le-dataplatform/src/test/python/data/profile-mulesoft-scoring.avro");
            HdfsUtils.copyLocalToHdfs(yarnConfiguration, profileMulesoftScoringFile.getAbsolutePath(),
                    profileMulesoftScoringPath);
        }

        private void modeling() throws Exception {
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

        private void prepareDataForScoring() throws Exception {
            uuid = getUuid();
            containerId = getContainerId();
            System.out.println("uid is " + uuid);

        }

        private String getUuid() throws Exception {
            List<String> dirList = HdfsUtils.getFilesForDir(yarnConfiguration, modelingModelPath);
            assertTrue(dirList.size() == 1, "There should be only one model generated.");
            return UuidUtils.parseUuid(dirList.get(0));
        }

        private String getContainerId() throws Exception {
            List<String> topDirs;
            topDirs = HdfsUtils.getFilesForDir(yarnConfiguration, modelingModelPath + uuid);
            String dir = topDirs.get(0);
            return dir.substring(dir.lastIndexOf("/") + 1);
        }

        private Model createModel(ModelDefinition modelDef) {
            Model m = new Model();
            m.setModelDefinition(modelDef);
            m.setName("Model Submission1");
            m.setTable("Q_PLS_ModelingMulesoft_Relaunch");
            m.setMetadataTable("EventMetadata");
            m.setTargetsList(Arrays.<String> asList(new String[] { "P1_Event" }));
            m.setKeyCols(Arrays.<String> asList(new String[] { "ModelingID" }));
            m.setCustomer(tenant);
            m.setDataFormat("avro");

            return m;
        }

        public void submitModel() throws Exception {
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

        private void scoring() throws Exception {
            ScoringConfiguration scoringConfig = new ScoringConfiguration();
            scoringConfig.setCustomer(tenant);
            scoringConfig.setSourceDataDir(dataPath);
            scoringConfig.setTargetResultDir(scorePath);
            scoringConfig.setModelGuids(Arrays.<String>asList(new String[]{"ms__" + uuid + "-PLS_model"}));
            scoringConfig.setUniqueKeyColumn("ModelingID");
            ApplicationId appId = scoringJobService.score(scoringConfig);
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
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            if (files.size() == 0) {
                throw new LedpException(LedpCode.LEDP_15003, new String[] { "avro" });
            }
            for (String file : files) {
                try {
                    List<GenericRecord> list = AvroUtils.getData(configuration, new Path(file));
                    newlist.addAll(list);
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
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

        @AfterMethod(enabled = true, lastTimeOnly = true, alwaysRun = true)
        public void afterEachTest() {
            try {
                HdfsUtils.rmdir(yarnConfiguration, path);
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        }

    }


