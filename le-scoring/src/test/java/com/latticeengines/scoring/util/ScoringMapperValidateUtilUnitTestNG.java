package com.latticeengines.scoring.util;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.fs.Path;
import org.codehaus.plexus.util.FileUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public class ScoringMapperValidateUtilUnitTestNG {

    private static final String MODEL_ID = ScoringTestUtils.generateRandomModelId();

    @Test(groups = "unit")
    public void testValidateTransformation() {
        // make up modelInfoMap
        Map<String, ModelAndRecordInfo.ModelInfo> modelInfoMap = new HashMap<String, ModelAndRecordInfo.ModelInfo>();
        ModelAndRecordInfo.ModelInfo modelInfo = new ModelAndRecordInfo.ModelInfo(MODEL_ID, 10);
        modelInfoMap.put(MODEL_ID, modelInfo);
        // make up modelAndLeadInfo
        ModelAndRecordInfo modelAndLeadInfo = new ModelAndRecordInfo();
        modelAndLeadInfo.setModelInfoMap(modelInfoMap);
        modelAndLeadInfo.setTotalRecordCountr(11);

        try {
            ScoringMapperValidateUtil.validateTransformation(modelAndLeadInfo);
            Assert.fail("should have thrown exception.");
        } catch (LedpException e) {
            Assert.assertEquals(e.getCode(), LedpCode.LEDP_20010);
        }
    }

    @Test(groups = "unit")
    public void testValidateLocalizedFiles() throws URISyntaxException {
        Map<String, URI> mockModels = new HashMap<>();
        try {
            ScoringMapperValidateUtil.validateLocalizedFiles(true, mockModels);
            Assert.fail("should have thrown exception.");
        } catch (LedpException e) {
            Assert.assertEquals(e.getCode(), LedpCode.LEDP_20020);
        }
        URI obj = new URI("");
        mockModels.put(MODEL_ID, obj);

        try {
            ScoringMapperValidateUtil.validateLocalizedFiles(false, mockModels);
            Assert.fail("should have thrown exception.");
        } catch (LedpException e) {
            Assert.assertEquals(e.getCode(), LedpCode.LEDP_20002);
        }
    }

    @Test(groups = "unit")
    public void testValidateDatatype() throws Exception {
        Map<String, JsonNode> models = new HashMap<>();
        String uuid = "60fd2fa4-9868-464e-a534-3205f52c41f0";
        URL modelUrl = ClassLoader.getSystemResource("com/latticeengines/scoring/models/" + uuid);
        String modelFileName = modelUrl.getFile();
        Path modelPath = new Path(modelFileName);
        FileUtils.copyURLToFile(modelUrl, new File(uuid));
        JsonNode modelJsonObj = ScoringMapperTransformUtil
                .parseFileContentToJsonNode(new URI(modelUrl.getFile() + "#" + uuid));
        String modelGuid = modelPath.getName();
        models.put(modelGuid, modelJsonObj);

        InputStream is = ClassLoader.getSystemResourceAsStream("com/latticeengines/scoring/data/datatype.avsc");
        JsonNode datatype = JsonUtils.getObjectMapper().readTree(is);
        try {
            ScoringMapperValidateUtil.validateDatatype(datatype, models.get(modelGuid), modelGuid);
        } catch (LedpException e1) {
            System.out.println(ExceptionUtils.getStackTrace(e1));
            Assert.fail("It should pass the validation without throwing any exceptions.");
        }

        try {
            ScoringMapperValidateUtil.validateDatatype(datatype, new ObjectMapper().createObjectNode(), "fakeModelID");
            Assert.fail("should have thrown exception.");
        } catch (LedpException e1) {
            assertTrue(e1.getCode() == LedpCode.LEDP_20001,
                    "Model not containing proper metadata cannot pass the validation.");
        }

        // datatype can only be 0 or 1
        assertTrue(datatype.has("SemrushRank"), "datatype contains the key 'SemrushRank'");
        ((ObjectNode) datatype).put("SemrushRank", 2l);
        try {
            ScoringMapperValidateUtil.validateDatatype(datatype, models.get(modelGuid), modelGuid);
            Assert.fail("should have thrown exception.");
        } catch (LedpException e1) {
            assertTrue(e1.getCode() == LedpCode.LEDP_20001, "Containing unknown datatype cannot pass the validation.");
        }
        // change the wrong value back to correct
        ((ObjectNode) datatype).put("SemrushRank", 0l);

        ((ObjectNode) datatype).remove("PercentileModel");
        assertFalse(datatype.has("PercentileModel"), "datatype does not contain the key 'PercentileModel'");
        try {
            ScoringMapperValidateUtil.validateDatatype(datatype, models.get(modelGuid), modelGuid);
            Assert.fail("should have thrown exception.");
        } catch (LedpException e1) {
            assertTrue(e1.getCode() == LedpCode.LEDP_20001, "Missing required column cannot pass the validation.");
        }

        ((ObjectNode) datatype).put("PercentileModel", 0l);
        assertTrue(datatype.has("PercentileModel"), "datatype contains the key 'PercentileModel'");
        try {
            ScoringMapperValidateUtil.validateDatatype(datatype, models.get(modelGuid), modelGuid);
            Assert.fail("should have thrown exception.");
        } catch (LedpException e1) {
            assertTrue(e1.getCode() == LedpCode.LEDP_20001, "Mismatching required column cannot pass the validation.");
        }
    }

}
