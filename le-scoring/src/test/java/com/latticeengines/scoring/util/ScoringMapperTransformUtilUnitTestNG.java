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

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.scoring.orchestration.service.ScoringDaemonService;

public class ScoringMapperTransformUtilUnitTestNG {

    private static final String SUMMARY = "Summary";
    private static final String LEAD_SERIALIZE_TYPE_KEY = "SerializedValueAndType";
    private final static String DATA_PATH = "com/latticeengines/scoring/data/";
    private final static String MODEL_PATH = "com/latticeengines/scoring/models/";
    private final static String PYTHON_PATH = "com/latticeengines/scoring/python/scoring.py";
    private final static String MODEL_SUPPORTED_FILE_PATH = "com/latticeengines/scoring/models/supportedFiles/";
    private final static String UUID = "60fd2fa4-9868-464e-a534-3205f52c41f0";
    private final static String MODEL_NAME = "2Checkout_relaunch_PLSModel_2015-03-19_15-37";
    private final static String PROPER_TEST_RECORD = "{\"LeadID\": \"837394\", \"ModelingID\": 113880, \"PercentileModel\": null, "
            + "\"FundingFiscalYear\": 123456789, \"BusinessFirmographicsParentEmployees\": 24, \"C_Job_Role1\": \"\", "
            + "\"BusinessSocialPresence\": \"True\", \"Model_GUID\": \"ms__60fd2fa4-9868-464e-a534-3205f52c41f0-Model_UI\"}";
    private final static String IMPROPER_TEST_RECORD_WITH_NO_LEAD_ID = "{\"ModelingID\": 113880, \"PercentileModel\": null, "
            + "\"FundingFiscalYear\": 123456789, \"BusinessFirmographicsParentEmployees\": 24, \"C_Job_Role1\": \"\", "
            + "\"BusinessSocialPresence\": \"True\", \"Model_GUID\": \"FAKE_PREFIX_2Checkout_relaunch_PLSModel_2015-03-19_15-37_model.json\"}";

    private Path modelPath;
    private Path pythonPath;
    private List<Path> localFilePaths;
    private JsonNode dataType;

    @BeforeClass(groups = "unit")
    public void setup() throws IOException {
        modelPath = new Path(ClassLoader.getSystemResource(MODEL_PATH + UUID).getFile());
        pythonPath = new Path(ClassLoader.getSystemResource(PYTHON_PATH).getFile());
        localFilePaths = new ArrayList<Path>();
        localFilePaths.add(modelPath);
        localFilePaths.add(pythonPath);
        Path datatypePath = new Path(ClassLoader.getSystemResource(DATA_PATH + "datatype.avsc").getFile());
        dataType = ScoringMapperTransformUtil.parseFileContentToJsonNode(datatypePath);
    }

    @Test(groups = "unit")
    public void testProcessLocalizedFiles() throws IOException {

        HashMap<String, JsonNode> models = ScoringMapperTransformUtil.processLocalizedFiles(localFilePaths
                .toArray(new Path[localFilePaths.size()]));
        Assert.assertNotNull(models);
        Assert.assertEquals(models.size(), 1);
        Assert.assertNotNull(models.get(UUID));
    }

    @Test(groups = "unit")
    public void testParseDatatypeFile() throws IOException {
        URL url = ClassLoader.getSystemResource(DATA_PATH + "mock_datatype.avsc");
        String fileName = url.getFile();
        Path path = new Path(fileName);
        JsonNode datatypeObj = ScoringMapperTransformUtil.parseFileContentToJsonNode(path);
        assertTrue(datatypeObj.size() == 7, "datatypeObj should have 7 objects");
        assertTrue(datatypeObj.get("ModelingID").asDouble() == 1L, "parseDatatypeFile should be successful");
    }

    @Test(groups = "unit")
    public void testParseModelFiles() throws IOException {
        String[] targetFiles = { "encoder.py", "pipeline.py", "pipelinefwk.py", "pipelinesteps.py", "scoringengine.py",
                "STPipelineBinary.p" };

        JsonNode modelJson = ScoringMapperTransformUtil.parseFileContentToJsonNode(modelPath);
        ScoringMapperTransformUtil.decodeSupportedFilesToFile(UUID, modelJson.get(ScoringDaemonService.MODEL));
        ScoringMapperTransformUtil.writeScoringScript(UUID, modelJson.get(ScoringDaemonService.MODEL));
        Assert.assertNotNull(modelJson);
        Assert.assertEquals(modelJson.get(ScoringDaemonService.AVERAGE_PROBABILITY).asDouble(), 0.011919253398255223);
        Assert.assertNotNull(modelJson.get(ScoringDaemonService.BUCKETS));
        Assert.assertNotNull(modelJson.get(ScoringDaemonService.CALIBRATION));
        Assert.assertNotNull(modelJson.get(ScoringDaemonService.INPUT_COLUMN_METADATA));
        Assert.assertNotNull(modelJson.get(ScoringDaemonService.MODEL));
        Assert.assertNotNull(modelJson.get(SUMMARY));
        Assert.assertEquals(modelJson.get(ScoringDaemonService.BUCKETS_NAME).asText(), MODEL_NAME);
        Assert.assertNotNull(modelJson.get(ScoringDaemonService.PERCENTILE_BUCKETS));
        for (int i = 0; i < targetFiles.length; i++) {
            System.out.println("Current target file is " + targetFiles[i]);
            assertTrue(compareFiles(targetFiles[i]), "parseModelFiles should be successful");
        }

    }

    private boolean compareFiles(String fileName) throws IOException {
        boolean filesAreSame = false;
        File newFile = new File(UUID + fileName);
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
    public void testTransformAndWriteLead() throws IOException, DecoderException {

        String expectedFileName = UUID + "-0";
        File expectedFile = new File(expectedFileName);
        if (expectedFile.exists()) {
            expectedFile.delete();
            System.out.println("leadInputFile has been deleted.");
        }
        Map<String, ModelAndRecordInfo.ModelInfo> modelInfoMap = new HashMap<String, ModelAndRecordInfo.ModelInfo>();
        Map<String, BufferedWriter> leadFileBufferMap = new HashMap<String, BufferedWriter>();

        HashMap<String, JsonNode> models = ScoringMapperTransformUtil.processLocalizedFiles(localFilePaths
                .toArray(new Path[localFilePaths.size()]));
        JsonNode jsonNode = new ObjectMapper().readTree(PROPER_TEST_RECORD);
        ScoringMapperTransformUtil.transformAndWriteRecord(jsonNode, dataType, modelInfoMap, leadFileBufferMap, models,
                10000, "ms__60fd2fa4-9868-464e-a534-3205f52c41f0-Model_UI", ScoringDaemonService.UNIQUE_KEY_COLUMN);

        Assert.assertNotNull(modelInfoMap);
        Assert.assertEquals(modelInfoMap.size(), 1);
        Assert.assertEquals(modelInfoMap.get(UUID).getModelGuid(),
                "ms__60fd2fa4-9868-464e-a534-3205f52c41f0-Model_UI");
        Assert.assertEquals(modelInfoMap.get(UUID).getRecordCount(), 1);
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

    private boolean transformedLeadIsCorrect(String transformedString) throws JsonProcessingException, IOException, DecoderException {

        JsonNode j = new ObjectMapper().readTree(transformedString);
        String recordId = new String(Hex.decodeHex(j.get("key").asText().toCharArray()), "UTF8");
        assertTrue(recordId.equals("837394"));
        ArrayNode arr = (ArrayNode) j.get("value");
        return (arr.size() == 194 && containsRightContents(arr));
    }

    private boolean containsRightContents(ArrayNode arr) {
        boolean result = true;
        for (int i = 0; i < arr.size() && result; i++) {
            JsonNode obj = arr.get(i);
            String key = obj.get("Key").asText();
            switch (key) {
            case "PercentileModel":
                if (!obj.get("Value").get(LEAD_SERIALIZE_TYPE_KEY).asText().equals("String|")) {
                    result = false;
                }
                break;
            case "FundingFiscalYear":
                if (!obj.get("Value").get(LEAD_SERIALIZE_TYPE_KEY).asText().equals("Float|'123456789'")) {
                    result = false;
                }
                break;
            case "BusinessFirmographicsParentEmployees":
                if (!obj.get("Value").get(LEAD_SERIALIZE_TYPE_KEY).asText().equals("Float|'24'")) {
                    result = false;
                }
                break;
            case "C_Job_Role1":
                if (!obj.get("Value").get(LEAD_SERIALIZE_TYPE_KEY).asText().equals("String|''")) {
                    result = false;
                }
                break;
            case "BusinessSocialPresence":
                if (!obj.get("Value").get(LEAD_SERIALIZE_TYPE_KEY).asText().equals("String|'True'")) {
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
    public void testTransformAndWriteLeadWithNegativeCases() throws IOException {
        Map<String, ModelAndRecordInfo.ModelInfo> modelInfoMap = new HashMap<String, ModelAndRecordInfo.ModelInfo>();
        Map<String, BufferedWriter> leadFileBufferMap = new HashMap<String, BufferedWriter>();

        HashMap<String, JsonNode> models = ScoringMapperTransformUtil.processLocalizedFiles(localFilePaths
                .toArray(new Path[localFilePaths.size()]));

        try {
            JsonNode jsonNode = new ObjectMapper().readTree(IMPROPER_TEST_RECORD_WITH_NO_LEAD_ID);
            ScoringMapperTransformUtil.transformAndWriteRecord(jsonNode, dataType, modelInfoMap, leadFileBufferMap, models,
                    10000, "ms__60fd2fa4-9868-464e-a534-3205f52c41f0-Model_UI", ScoringDaemonService.UNIQUE_KEY_COLUMN);
            Assert.fail("Should have thrown expcetion.");
        } catch (LedpException e) {
            Assert.assertEquals(e.getCode(), LedpCode.LEDP_20003);
        }
    }
}
