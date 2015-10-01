package com.latticeengines.scoring.util;

import static org.testng.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public class ScoringMapperTransformUtilUnitTestNG {

    private static final String SUMMARY = "Summary";
    private static final String LEAD_SERIALIZE_TYPE_KEY = "SerializedValueAndType";
    private final static String DATA_PATH = "com/latticeengines/scoring/data/";
    private final static String MODEL_PATH = "com/latticeengines/scoring/models/";
    private final static String PYTHON_PATH = "com/latticeengines/scoring/python/scoring.py";
    private final static String MODEL_SUPPORTED_FILE_PATH = "com/latticeengines/scoring/models/supportedFiles/";
    private final static String MODEL_ID = "2Checkout_relaunch_PLSModel_2015-03-19_15-37_model.json";
    private final static String MODEL_NAME = "2Checkout_relaunch_PLSModel_2015-03-19_15-37";
    private final static String PROPER_TEST_RECORD = "{\"LeadID\": \"837394\", \"ModelingID\": 113880, \"PercentileModel\": null, "
            + "\"FundingFiscalYear\": 123456789, \"BusinessFirmographicsParentEmployees\": 24, \"C_Job_Role1\": \"\", "
            + "\"BusinessSocialPresence\": \"True\", \"Model_GUID\": \"FAKE_PREFIX_2Checkout_relaunch_PLSModel_2015-03-19_15-37_model.json\"}";

    private final static String IMPROPER_TEST_RECORD = "{\"LeadID\": \"837395\", \"ModelingID\": 113881, \"PercentileModel\": null, "
            + "\"FundingFiscalYear\": 123456789, \"BusinessFirmographicsParentEmployees\": 36, \"C_Job_Role1\": \"\", "
            + "\"BusinessSocialPresence\": \"True\", \"Model_GUID\": \"FAKE_PREFIX_SOME_RANDOM_MODEL_ID\"}";

    private Path datatypePath;
    private Path modelPath;
    private Path pythonPath;
    private List<Path> localFilePaths;

    @BeforeClass(groups = "unit")
    public void setup() {
        datatypePath = new Path(ClassLoader.getSystemResource(DATA_PATH + "datatype.avsc").getFile());
        modelPath = new Path(ClassLoader.getSystemResource(MODEL_PATH + MODEL_ID).getFile());
        pythonPath = new Path(ClassLoader.getSystemResource(PYTHON_PATH).getFile());
        localFilePaths = new ArrayList<Path>();
        localFilePaths.add(datatypePath);
        localFilePaths.add(modelPath);
        localFilePaths.add(pythonPath);
    }

    @Test(groups = "unit")
    public void testProcessLocalizedFiles() throws IOException, ParseException {

        LocalizedFiles localizedFiles = ScoringMapperTransformUtil.processLocalizedFiles(localFilePaths
                .toArray(new Path[localFilePaths.size()]));
        Assert.assertNotNull(localizedFiles);
        Assert.assertNotNull(localizedFiles.getDatatype());
        Assert.assertEquals(localizedFiles.getModels().size(), 1);
        Assert.assertNotNull(localizedFiles.getModels().get(MODEL_ID));
    }

    @Test(groups = "unit")
    public void testParseDatatypeFile() throws IOException, ParseException {
        URL url = ClassLoader.getSystemResource(DATA_PATH + "mock_datatype.avsc");
        String fileName = url.getFile();
        Path path = new Path(fileName);
        JSONObject datatypeObj = ScoringMapperTransformUtil.parseDatatypeFile(path);
        assertTrue(datatypeObj.size() == 7, "datatypeObj should have 7 objects");
        assertTrue(datatypeObj.get("ModelingID").equals(new Long(1)), "parseDatatypeFile should be successful");
    }

    @Test(groups = "unit")
    public void testParseModelFiles() throws IOException, ParseException {
        String[] targetFiles = { "encoder.py", "pipeline.py", "pipelinefwk.py", "pipelinesteps.py", "scoringengine.py",
                "STPipelineBinary.p" };

        JSONObject modelJson = ScoringMapperTransformUtil.parseModelFiles(modelPath);
        Assert.assertNotNull(modelJson);
        Assert.assertEquals(modelJson.get(ScoringMapperPredictUtil.AVERAGE_PROBABILITY), 0.011919253398255223);
        Assert.assertNotNull(modelJson.get(ScoringMapperPredictUtil.BUCKETS));
        Assert.assertNotNull(modelJson.get(ScoringMapperPredictUtil.CALIBRATION));
        Assert.assertNotNull(modelJson.get(ScoringMapperTransformUtil.INPUT_COLUMN_METADATA));
        Assert.assertNotNull(modelJson.get(ScoringMapperTransformUtil.MODEL));
        Assert.assertNotNull(modelJson.get(SUMMARY));
        Assert.assertEquals(modelJson.get(ScoringMapperPredictUtil.BUCKETS_NAME), MODEL_NAME);
        Assert.assertNotNull(modelJson.get(ScoringMapperPredictUtil.PERCENTILE_BUCKETS));
        for (int i = 0; i < targetFiles.length; i++) {
            System.out.println("Current target file is " + targetFiles[i]);
            assertTrue(compareFiles(targetFiles[i]), "parseModelFiles should be successful");
        }

    }

    private boolean compareFiles(String fileName) throws IOException {
        boolean filesAreSame = false;
        File newFile = new File(MODEL_ID + fileName);
        URL url = ClassLoader.getSystemResource(MODEL_SUPPORTED_FILE_PATH + fileName);
        File oldFile = new File(url.getFile());
        filesAreSame = compareFilesLineByLine(newFile, oldFile);
        return filesAreSame;
    }

    private boolean compareFilesLineByLine(File file1, File file2) throws IOException {
        boolean filesAreSame = true;
        LineNumberReader reader1 = new LineNumberReader(new FileReader(file1));
        LineNumberReader reader2 = new LineNumberReader(new FileReader(file2));
        String line1 = reader1.readLine();
        String line2 = reader2.readLine();
        while (line1 != null && line2 != null) {
            if (!line1.equals(line2)) {
                System.out.println("File \"" + file1 + "\" and file \"" + file2 + "\" differ at line "
                        + reader1.getLineNumber() + ":" + "\n" + line1 + "\n" + line2);
                filesAreSame = false;
                break;
            }
            line1 = reader1.readLine();
            line2 = reader2.readLine();
        }
        if ((line1 == null && line2 != null) || (line1 != null && line2 == null)) {
            filesAreSame = false;
        }
        reader1.close();
        reader2.close();
        return filesAreSame;
    }

    @Test(groups = "unit")
    public void testTransformAndWriteLead() throws IOException, ParseException {

        String expectedFileName = MODEL_ID + "-0";
        File expectedFile = new File(expectedFileName);
        if (expectedFile.exists()) {
            expectedFile.delete();
            System.out.println("leadInputFile has been deleted.");
        }
        Map<String, ModelAndLeadInfo.ModelInfo> modelInfoMap = new HashMap<String, ModelAndLeadInfo.ModelInfo>();
        Map<String, BufferedWriter> leadFileBufferMap = new HashMap<String, BufferedWriter>();

        LocalizedFiles localizedFiles = ScoringMapperTransformUtil.processLocalizedFiles(localFilePaths
                .toArray(new Path[localFilePaths.size()]));
        ScoringMapperTransformUtil.transformAndWriteLead(PROPER_TEST_RECORD, modelInfoMap, leadFileBufferMap,
                localizedFiles, 10000);

        Assert.assertNotNull(modelInfoMap);
        Assert.assertEquals(modelInfoMap.size(), 1);
        Assert.assertEquals(modelInfoMap.get(MODEL_ID).getModelId(),
                "FAKE_PREFIX_2Checkout_relaunch_PLSModel_2015-03-19_15-37_model.json");
        Assert.assertEquals(modelInfoMap.get(MODEL_ID).getLeadNumber(), 1);
        Assert.assertNotNull(leadFileBufferMap);
        Assert.assertEquals(leadFileBufferMap.size(), 1);
        Assert.assertNotNull(leadFileBufferMap.get(expectedFileName));

        Assert.assertTrue(expectedFile.exists());
        System.out.println("The test full path is " + expectedFile.getAbsolutePath());
        leadFileBufferMap.get(expectedFileName).close();

        String leadInputFileContents = FileUtils.readFileToString(expectedFile);
        Assert.assertTrue(transformedLeadIsCorrect(leadInputFileContents),
                "The lead input file does not contain the right contents.");

        Assert.assertTrue(expectedFile.delete());
    }

    @Test(groups = "unit")
    private boolean transformedLeadIsCorrect(String transformedString) throws ParseException {

        JSONParser parser = new JSONParser();
        JSONObject j = (JSONObject) parser.parse(transformedString);
        assertTrue(j.get("key").equals("837394"));
        JSONArray arr = (JSONArray) j.get("value");
        return (arr.size() == 194 && containsRightContents(arr));
    }

    private boolean containsRightContents(JSONArray arr) {
        boolean result = true;
        for (int i = 0; i < arr.size() && result; i++) {
            JSONObject obj = (JSONObject) arr.get(i);
            String key = (String) obj.get("Key");
            switch (key) {
            case "PercentileModel":
                if (!((String) ((JSONObject) obj.get("Value")).get(LEAD_SERIALIZE_TYPE_KEY)).equals("String|")) {
                    result = false;
                }
                break;
            case "FundingFiscalYear":
                if (!((String) ((JSONObject) obj.get("Value")).get(LEAD_SERIALIZE_TYPE_KEY))
                        .equals("Float|'123456789'")) {
                    result = false;
                }
                break;
            case "BusinessFirmographicsParentEmployees":
                if (!((String) ((JSONObject) obj.get("Value")).get(LEAD_SERIALIZE_TYPE_KEY)).equals("Float|'24'")) {
                    result = false;
                }
                break;
            case "C_Job_Role1":
                if (!((String) ((JSONObject) obj.get("Value")).get(LEAD_SERIALIZE_TYPE_KEY)).equals("String|''")) {
                    result = false;
                }
                break;
            case "BusinessSocialPresence":
                if (!((String) ((JSONObject) obj.get("Value")).get(LEAD_SERIALIZE_TYPE_KEY)).equals("String|'True'")) {
                    result = false;
                }
                break;
            default:
                break;
            }
        }
        return result;
    }

    @Test(groups = "unit")
    public void testProcessBitValue() {

        assertTrue(ScoringMapperTransformUtil.processBitValue("Float", "true").equals("1"));
        assertTrue(ScoringMapperTransformUtil.processBitValue("Float", "false").equals("0"));
        assertTrue(ScoringMapperTransformUtil.processBitValue("Float", "2").equals("2"));
        assertTrue(ScoringMapperTransformUtil.processBitValue("String", "true").equals("true"));
        assertTrue(ScoringMapperTransformUtil.processBitValue("String", "false").equals("false"));
    }

    @Test(groups = "unit")
    public void testTransformAndWriteLeadWithUnmatchedModel() throws IOException, ParseException {
        Map<String, ModelAndLeadInfo.ModelInfo> modelInfoMap = new HashMap<String, ModelAndLeadInfo.ModelInfo>();
        Map<String, BufferedWriter> leadFileBufferMap = new HashMap<String, BufferedWriter>();

        LocalizedFiles localizedFiles = ScoringMapperTransformUtil.processLocalizedFiles(localFilePaths
                .toArray(new Path[localFilePaths.size()]));
        try {
            ScoringMapperTransformUtil.transformAndWriteLead(IMPROPER_TEST_RECORD, modelInfoMap, leadFileBufferMap,
                    localizedFiles, 10000);
            Assert.fail("Should have thrown expcetion.");
        } catch (LedpException e) {
            Assert.assertEquals(e.getCode(), LedpCode.LEDP_20007);
        }
    }
}
