package com.latticeengines.scoring.runtime.mapreduce;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

import com.google.common.base.Joiner;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStatus;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStep;
import com.latticeengines.scoring.entitymanager.ScoringCommandEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandResultEntityMgr;
import com.latticeengines.scoring.functionalframework.ScoringFunctionalTestNGBase;
import com.latticeengines.scoring.service.ScoringStepYarnProcessor;
import com.latticeengines.scoring.service.impl.ScoringStepYarnProcessorImplTestNG;

public class ScoringComparisonAgainstProdForSingleModelTestNG extends ScoringFunctionalTestNGBase {

    private static final double EPS = 1e-6;
    private static final String modelID = "2Checkout_relaunch_PLSModel_2015-03-19_15-37_model.json";

    private static final Log log = LogFactory.getLog(ScoringStepYarnProcessorImplTestNG.class);

    @Autowired
    private ScoringCommandEntityMgr scoringCommandEntityMgr;

    @Autowired
    private ScoringStepYarnProcessor scoringStepYarnProcessor;

    @Value("${dataplatform.customer.basedir}")
    private String customerBaseDir;

    @Autowired
    private Configuration yarnConfiguration;

    private static final String customer = "Nutanix";

    private static String tenant;

    @Value("${scoring.test.table}")
    private String testInputTable;

    @Autowired
    private ScoringCommandResultEntityMgr scoringCommandResultEntityMgr;

    private String inputLeadsTable;

    private String modelPath;

    private String path;

    private String scorePath;

    private static final Joiner dotJoiner = Joiner.on('.').skipNulls();

    @BeforeMethod(groups = "functional")
    public void beforeMethod() throws Exception {
    }

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        inputLeadsTable = getClass().getSimpleName() + "_LeadsTable";
        tenant = dotJoiner.join(customer, customer, "Production");

        // upload lead files to HDFS
        URL url1 = ClassLoader.getSystemResource("com/latticeengines/scoring/data/"
                + "2Checkout_ScoringComparisonAgainstProdForSingleModelTestNG-00000.avro");
        URL url3 = ClassLoader.getSystemResource("com/latticeengines/scoring/data/"
                + "2Checkout_ScoringComparisonAgainstProdForSingleModelTestNG-00002.avro");
        URL url4 = ClassLoader.getSystemResource("com/latticeengines/scoring/data/"
                + "2Checkout_ScoringComparisonAgainstProdForSingleModelTestNG-00003.avro");

        path = customerBaseDir + "/" + tenant + "/scoring/" + inputLeadsTable + "/data";
        scorePath = customerBaseDir + "/" + tenant + "/scoring/" + inputLeadsTable + "/scores";
        HdfsUtils.mkdir(yarnConfiguration, path);
        String dataPath1 = path + "/1.avro";
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, url1.getFile(), dataPath1);
        String dataPath3 = path + "/3.avro";
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, url3.getFile(), dataPath3);
        String dataPath4 = path + "/4.avro";
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, url4.getFile(), dataPath4);

        URL modelSummaryUrl = ClassLoader.getSystemResource("com/latticeengines/scoring/models/" + modelID);
        modelPath = customerBaseDir + "/" + tenant + "/models/" + inputLeadsTable
                + "/1e8e6c34-80ec-4f5b-b979-e79c8cc6bec3/1429553747321_0004";
        HdfsUtils.mkdir(yarnConfiguration, modelPath);
        String filePath = modelPath + "/model.json";
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), filePath);
    }

    // compare the results
    private boolean compareEvaluationResults() {
        boolean evaluationIsSame = false;
        List<GenericRecord> newlist = null;
        List<GenericRecord> oldlist = null;

        // load new scores from HDFS
        newlist = loadHDFSAvroFiles(new Configuration(), scorePath);

        // load existing scores from testing file
        URL url = ClassLoader.getSystemResource("com/latticeengines/scoring/results/"
                + "2Checkout_ScoringComparisonAgainstProdForSingleModelTestNG-00000.avro");
        String fileName = url.getFile();
        oldlist = loadLocalAvroFiles(fileName);
        evaluationIsSame = compareJsonResults(newlist, oldlist);
        return evaluationIsSame;
    }

    private ArrayList<GenericRecord> loadHDFSAvroFiles(Configuration configuration, String hdfsDir) {
        System.out.println("hdfsDir is " + hdfsDir);
        ArrayList<GenericRecord> newlist = new ArrayList<GenericRecord>();
        List<String> files = null;
        try {
            files = HdfsUtils.getFilesForDir(configuration, hdfsDir);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
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

    private List<GenericRecord> loadLocalAvroFiles(String localDir) {
        List<GenericRecord> newlist = new ArrayList<GenericRecord>();
        File localAvroFile = new File(localDir);
        FileReader<GenericRecord> reader;
        GenericDatumReader<GenericRecord> fileReader = new GenericDatumReader<>();
        try {
            reader = DataFileReader.openReader(localAvroFile, fileReader);
            for (GenericRecord datum : reader) {
                newlist.add(datum);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return newlist;
    }

    private boolean compareJsonResults(List<GenericRecord> newResults, List<GenericRecord> oldResults) {

        boolean resultsAreSame = true;
        if (newResults.size() != oldResults.size()) {
            System.out.println("size not the same");
            System.out.println("newResults.size() is " + newResults.size());
            System.out.println("oldResults.size() is " + oldResults.size());
            resultsAreSame = false;
        } else {
            HashMap<String, GenericRecord> resultMap = new HashMap<String, GenericRecord>();
            for (int i = 0; i < newResults.size(); i++) {
                GenericRecord newResult = newResults.get(i);
                String key = newResult.get("LeadID").toString() + newResult.get("Play_Display_Name").toString();
                resultMap.put(key, newResult);
            }

            for (int i = 0; i < oldResults.size(); i++) {
                GenericRecord oldResult = oldResults.get(i);
                String key = oldResult.get("LeadID").toString() + oldResult.get("Play_Display_Name").toString();
                if (!resultMap.containsKey(key)) {
                    System.out.println("key has not the same");
                    resultsAreSame = false;
                    break;
                } else {
                    if (!compareTwoRecord(resultMap.get(key), oldResult)) {
                        resultsAreSame = false;
                        break;
                    }
                }
            }
        }
        return resultsAreSame;
    }

    private boolean compareTwoRecord(GenericRecord newRecord, GenericRecord oldRecord) {
        boolean recordsAreSame = true;
        String[] columns = { "Bucket_Display_Name", "Lift", "Percentile", "Probability", "RawScore", "Score" };
        for (String column : columns) {
            switch (column) {
            case "Bucket_Display_Name":
                if (!newRecord.get(column).equals(oldRecord.get(column))) {
                    System.out.println("come to the " + column);
                    recordsAreSame = false;
                }
                break;
            case "Lift":
                if (!newRecord.get(column).equals(oldRecord.get(column))) {
                    System.out.println("come to the " + column);
                    recordsAreSame = false;
                }
                break;
            case "Percentile":
                if (!newRecord.get(column).equals(oldRecord.get(column))) {
                    System.out.println("come to the " + column);
                    recordsAreSame = false;
                }
                break;
            case "Probability":
                if (!newRecord.get(column).equals(oldRecord.get(column))) {
                    System.out.println("come to the " + column);
                    recordsAreSame = false;
                }
                break;
            case "RawScore":
                if (Math.abs((Double) newRecord.get(column) - (Double) oldRecord.get(column)) > EPS) {
                    System.out.println("come to the " + column);
                    recordsAreSame = false;
                }
                break;
            case "Score":
                if (!newRecord.get(column).equals(oldRecord.get(column))) {
                    System.out.println("come to the " + column);
                    recordsAreSame = false;
                }
                break;
            }
        }
        return recordsAreSame;
    }

    @Test(groups = "functional")
    public void loadAndScore() throws Exception {
        ScoringCommand scoringCommand = new ScoringCommand(customer, ScoringCommandStatus.POPULATED, inputLeadsTable,
                0, 4352, new Timestamp(System.currentTimeMillis()));
        scoringCommandEntityMgr.create(scoringCommand);
        // trigger the scoring
        ApplicationId appId = scoringStepYarnProcessor.executeYarnStep(scoringCommand, ScoringCommandStep.SCORE_DATA);
        waitForSuccess(appId, ScoringCommandStep.SCORE_DATA);

        // compare the results
        assertTrue(compareEvaluationResults());
    }

    private void waitForSuccess(ApplicationId appId, ScoringCommandStep step) throws Exception {
        FinalApplicationStatus status = waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
        log.info(step + ": appId succeeded: " + appId.toString());
    }

    @AfterMethod(enabled = true, lastTimeOnly = true, alwaysRun = true)
    public void afterEachTest() {
        try {
            HdfsUtils.rmdir(yarnConfiguration, path);
            HdfsUtils.rmdir(yarnConfiguration, scorePath);
            HdfsUtils.rmdir(yarnConfiguration, modelPath);
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

}
