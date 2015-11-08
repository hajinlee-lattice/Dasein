package com.latticeengines.scoring.util;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public class ScoringMapperValidateUtilUnitTestNG {

    private static final String MODEL_ID = "2Checkout_relaunch_PLSModel_2015-03-19_15-37_model.json";

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
    public void testValidateLocalizedFiles() {
        Map<String, JsonNode> mockModels = new HashMap<>();

        try {
            ScoringMapperValidateUtil.validateLocalizedFiles(true, true, mockModels);
            Assert.fail("should have thrown exception.");
        } catch (LedpException e) {
            Assert.assertEquals(e.getCode(), LedpCode.LEDP_20020);
        }

        JsonNode obj = new ObjectMapper().createObjectNode();
        mockModels.put(MODEL_ID, obj);

        try {
            ScoringMapperValidateUtil.validateLocalizedFiles(false, true, mockModels);
            Assert.fail("should have thrown exception.");
        } catch (LedpException e) {
            Assert.assertEquals(e.getCode(), LedpCode.LEDP_20002);
        }

        try {
            ScoringMapperValidateUtil.validateLocalizedFiles(true, false, mockModels);
            Assert.fail("should have thrown exception.");
        } catch (LedpException e) {
            Assert.assertEquals(e.getCode(), LedpCode.LEDP_20006);
        }

        try {
            ScoringMapperValidateUtil.validateLocalizedFiles(true, true, mockModels);
        } catch (LedpException e) {
            Assert.fail("should NOT have thrown exception.");
        }

    }

    @Test(groups = "unit")
    public void testValidateDatatype() throws IOException {
        HashMap<String, JsonNode> models = new HashMap<>();
        URL modelUrl = ClassLoader
                .getSystemResource("com/latticeengines/scoring/models/60fd2fa4-9868-464e-a534-3205f52c41f0");
        String modelFileName = modelUrl.getFile();
        Path modelPath = new Path(modelFileName);
        JsonNode modelJsonObj = ScoringMapperTransformUtil.parseFileContentToJsonNode(modelPath);
        String modelGuid = modelPath.getName();
        models.put(modelGuid, modelJsonObj);

        URL datatypeUrl = ClassLoader.getSystemResource("com/latticeengines/scoring/data/" + "datatype.avsc");
        String datatypeFileName = datatypeUrl.getFile();
        Path datatypePath = new Path(datatypeFileName);
        JsonNode datatype = ScoringMapperTransformUtil.parseFileContentToJsonNode(datatypePath);
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
