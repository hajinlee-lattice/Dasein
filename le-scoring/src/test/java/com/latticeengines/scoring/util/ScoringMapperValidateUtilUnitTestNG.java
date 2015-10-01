package com.latticeengines.scoring.util;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public class ScoringMapperValidateUtilUnitTestNG {

    private static final String MODEL_ID = "2Checkout_relaunch_PLSModel_2015-03-19_15-37_model.json";

    @Test(groups = "unit")
    public void testValidateTransformation() {
        // make up modelInfoMap
        Map<String, ModelAndLeadInfo.ModelInfo> modelInfoMap = new HashMap<String, ModelAndLeadInfo.ModelInfo>();
        ModelAndLeadInfo.ModelInfo modelInfo = new ModelAndLeadInfo.ModelInfo(MODEL_ID, 10);
        modelInfoMap.put(MODEL_ID, modelInfo);
        // make up modelAndLeadInfo
        ModelAndLeadInfo modelAndLeadInfo = new ModelAndLeadInfo();
        modelAndLeadInfo.setModelInfoMap(modelInfoMap);
        modelAndLeadInfo.setTotalleadNumber(11);

        try {
            ScoringMapperValidateUtil.validateTransformation(modelAndLeadInfo);
            Assert.fail("should have thrown exception.");
        } catch (LedpException e) {
            Assert.assertEquals(e.getCode(), LedpCode.LEDP_20010);
        }
    }

    @Test(groups = "unit")
    public void testValidateLocalizedFiles() {
        Map<String, JSONObject> mockModels = new HashMap<String, JSONObject>();

        try {
            ScoringMapperValidateUtil.validateLocalizedFiles(true, true, mockModels);
            Assert.fail("should have thrown exception.");
        } catch (LedpException e) {
            Assert.assertEquals(e.getCode(), LedpCode.LEDP_20020);
        }

        JSONObject obj = new JSONObject();
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

    @SuppressWarnings("unchecked")
    @Test(groups = "unit")
    public void testValidateDatatype() throws IOException, ParseException {
        HashMap<String, JSONObject> models = new HashMap<String, JSONObject>();
        URL modelUrl = ClassLoader
                .getSystemResource("com/latticeengines/scoring/models/2Checkout_relaunch_PLSModel_2015-03-19_15-37_model.json");
        String modelFileName = modelUrl.getFile();
        Path modelPath = new Path(modelFileName);
        JSONObject modelJsonObj = ScoringMapperTransformUtil.parseModelFiles(modelPath);
        String modelGuid = modelPath.getName();
        models.put(modelGuid, modelJsonObj);

        JSONObject datatype = null;
        URL datatypeUrl = ClassLoader.getSystemResource("com/latticeengines/scoring/data/" + "datatype.avsc");
        String datatypeFileName = datatypeUrl.getFile();
        Path datatypePath = new Path(datatypeFileName);
        datatype = ScoringMapperTransformUtil.parseDatatypeFile(datatypePath);
        try {
            ScoringMapperValidateUtil.validateDatatype(datatype, models.get(modelGuid), modelGuid);
        } catch (LedpException e1) {
            Assert.fail("It should pass the validation without throwing any exceptions.");
        }

        try {
            ScoringMapperValidateUtil.validateDatatype(datatype, new JSONObject(), "fakeModelID");
            Assert.fail("should have thrown exception.");
        } catch (LedpException e1) {
            assertTrue(e1.getCode() == LedpCode.LEDP_20001,
                    "Model not containing proper metadata cannot pass the validation.");
        }

        // datatype can only be 0 or 1
        assertTrue(datatype.containsKey("SemrushRank"), "datatype contains the key 'SemrushRank'");
        datatype.put("SemrushRank", 2l);
        try {
            ScoringMapperValidateUtil.validateDatatype(datatype, models.get(modelGuid), modelGuid);
            Assert.fail("should have thrown exception.");
        } catch (LedpException e1) {
            assertTrue(e1.getCode() == LedpCode.LEDP_20001, "Containing unknown datatype cannot pass the validation.");
        }
        // change the wrong value back to correct
        datatype.put("SemrushRank", 0l);

        datatype.remove("PercentileModel");
        assertFalse(datatype.containsKey("PercentileModel"), "datatype does not contain the key 'PercentileModel'");
        try {
            ScoringMapperValidateUtil.validateDatatype(datatype, models.get(modelGuid), modelGuid);
            Assert.fail("should have thrown exception.");
        } catch (LedpException e1) {
            assertTrue(e1.getCode() == LedpCode.LEDP_20001, "Missing required column cannot pass the validation.");
        }

        datatype.put("PercentileModel", 0l);
        assertTrue(datatype.containsKey("PercentileModel"), "datatype contains the key 'PercentileModel'");
        try {
            ScoringMapperValidateUtil.validateDatatype(datatype, models.get(modelGuid), modelGuid);
            Assert.fail("should have thrown exception.");
        } catch (LedpException e1) {
            assertTrue(e1.getCode() == LedpCode.LEDP_20001, "Mismatching required column cannot pass the validation.");
        }
    }

}
