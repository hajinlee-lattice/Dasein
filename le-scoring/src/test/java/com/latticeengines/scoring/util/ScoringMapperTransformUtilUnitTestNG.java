package com.latticeengines.scoring.util;

import static org.testng.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.LineNumberReader;
import java.net.URI;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.scoring.orchestration.service.ScoringDaemonService;
import com.latticeengines.scoring.runtime.mapreduce.ScoreContext;

public class ScoringMapperTransformUtilUnitTestNG {

    private static final String SUMMARY = "Summary";
    private static final String LEAD_SERIALIZE_TYPE_KEY = "SerializedValueAndType";
    private static final String DATA_PATH = "com/latticeengines/scoring/data/";
    private static final String MODEL_PATH = "com/latticeengines/scoring/models/";
    private static final String PYTHON_PATH = "com/latticeengines/scoring/python/testscoring.py";
    private static final String MODEL_SUPPORTED_FILE_PATH = "com/latticeengines/scoring/models/supportedFiles/";
    private static final String UUID = "60fd2fa4-9868-464e-a534-3205f52c41f0";
    private static final String MODEL_NAME = "2Checkout_relaunch_PLSModel_2015-03-19_15-37";
    private static final String PROPER_TEST_RECORD = "{\"LeadID\": \"837394\", \"ModelingID\": 113880, \"PercentileModel\": null, "
            + "\"FundingFiscalYear\": 123456789, \"BusinessFirmographicsParentEmployees\": 24, \"C_Job_Role1\": \"\", "
            + "\"BusinessSocialPresence\": \"True\", \"Model_GUID\": \"ms__60fd2fa4-9868-464e-a534-3205f52c41f0-Model_UI\"}";
    private static final String IMPROPER_TEST_RECORD_WITH_NO_LEAD_ID = "{\"ModelingID\": 113880, \"PercentileModel\": null, "
            + "\"FundingFiscalYear\": 123456789, \"BusinessFirmographicsParentEmployees\": 24, \"C_Job_Role1\": \"\", "
            + "\"BusinessSocialPresence\": \"True\", \"Model_GUID\": \"FAKE_PREFIX_2Checkout_relaunch_PLSModel_2015-03-19_15-37_model.json\"}";

    private URI modelPath;
    private URI pythonPath;
    private List<URI> localFilePaths;
    private JsonNode dataType;

    @BeforeClass(groups = "unit")
    public void setup() throws Exception {
        URL modelPathUrl = ClassLoader.getSystemResource(MODEL_PATH + UUID);
        URL pythonPathUrl = ClassLoader.getSystemResource(PYTHON_PATH);
        FileUtils.copyURLToFile(modelPathUrl, new File(UUID));
        FileUtils.copyURLToFile(pythonPathUrl, new File("testscoring.py"));
        modelPath = new URI(modelPathUrl.getFile() + "#" + UUID);
        pythonPath = new URI(pythonPathUrl.getFile());
        localFilePaths = new ArrayList<URI>();
        localFilePaths.add(modelPath);
        localFilePaths.add(pythonPath);
        InputStream is = ClassLoader.getSystemResourceAsStream(DATA_PATH + "datatype.avsc");
        dataType = JsonUtils.getObjectMapper().readTree(is);
    }

    @Test(groups = "unit")
    public void testProcessLocalizedFiles() throws IOException {

        Map<String, JsonNode> models = ScoringMapperTransformUtil.processLocalizedFiles(modelPath);
        Assert.assertNotNull(models);
        Assert.assertEquals(models.size(), 1);
        Assert.assertNotNull(models.get(UUID));
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

        Map<String, JsonNode> models = ScoringMapperTransformUtil.processLocalizedFiles(modelPath);
        JsonNode jsonNode = new ObjectMapper().readTree(PROPER_TEST_RECORD);
        ScoreContext scoreContext = new ScoreContext();
        scoreContext.recordFileBufferMap = leadFileBufferMap;
        scoreContext.modelInfoMap = modelInfoMap;
        scoreContext.recordFileThreshold = 10000;
        scoreContext.uniqueKeyColumn = ScoringDaemonService.UNIQUE_KEY_COLUMN;
        scoreContext.uuidToModeId.put(UUID, "ms__60fd2fa4-9868-464e-a534-3205f52c41f0-Model_UI");
        ScoringMapperTransformUtil.transformAndWriteRecord(UUID, scoreContext, jsonNode, dataType, models);

        Assert.assertNotNull(modelInfoMap);
        Assert.assertEquals(modelInfoMap.size(), 1);
        Assert.assertEquals(modelInfoMap.get(UUID).getModelGuid(), "ms__60fd2fa4-9868-464e-a534-3205f52c41f0-Model_UI");
        Assert.assertEquals(modelInfoMap.get(UUID).getRecordCount(), 1);
        Assert.assertNotNull(leadFileBufferMap);
        Assert.assertEquals(leadFileBufferMap.size(), 1);
        Assert.assertNotNull(leadFileBufferMap.get(expectedFileName));

        Assert.assertTrue(expectedFile.exists());
        System.out.println("The test full path is " + expectedFile.getAbsolutePath());
        leadFileBufferMap.get(expectedFileName).close();

        String leadInputFileContents = FileUtils.readFileToString(expectedFile, Charset.defaultCharset());
        Assert.assertTrue(transformedLeadIsCorrect(leadInputFileContents),
                "The lead input file does not contain the right contents.");

        Assert.assertTrue(expectedFile.delete());
    }

    private boolean transformedLeadIsCorrect(String transformedString)
            throws JsonProcessingException, IOException, DecoderException {

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
        Map<String, JsonNode> models = ScoringMapperTransformUtil.processLocalizedFiles(modelPath);

        try {
            JsonNode jsonNode = new ObjectMapper().readTree(IMPROPER_TEST_RECORD_WITH_NO_LEAD_ID);
            ScoreContext scoreContext = new ScoreContext();
            scoreContext.modelInfoMap = modelInfoMap;
            scoreContext.recordFileBufferMap = leadFileBufferMap;
            scoreContext.recordFileThreshold = 10000;
            scoreContext.uniqueKeyColumn = ScoringDaemonService.UNIQUE_KEY_COLUMN;
            ScoringMapperTransformUtil.transformAndWriteRecord(UUID, scoreContext, jsonNode, dataType, models);
            Assert.fail("Should have thrown expcetion.");
        } catch (LedpException e) {
            Assert.assertEquals(e.getCode(), LedpCode.LEDP_20003);
        }
    }
}
