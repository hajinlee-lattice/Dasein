package com.latticeengines.scoring.util;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public class ScoringMapperValidateUtilUnitTestNG {

    @SuppressWarnings("unchecked")
    @Test(groups = "unit")
    public void testProcessScoreFiles() throws IOException, ParseException {
        HashMap<String, JSONObject> models = new HashMap<String, JSONObject>();
        URL modelUrl = ClassLoader
                .getSystemResource("com/latticeengines/scoring/models/2Checkout_relaunch_PLSModel_2015-03-19_15-37_model.json");
        String modelFileName = modelUrl.getFile();
        Path modelPath = new Path(modelFileName);
        ScoringMapperTransformUtil.parseModelFiles(models, modelPath);

        JSONObject datatype = null;
        URL datatypeUrl = ClassLoader.getSystemResource("com/latticeengines/scoring/data/"
                + "2Checkout_relaunch_Q_PLS_Scoring_Incremental_1336210_2015-05-15_005244-datatype.avsc");
        String datatypeFileName = datatypeUrl.getFile();
        Path datatypePath = new Path(datatypeFileName);
        datatype = ScoringMapperTransformUtil.parseDatatypeFile(datatypePath);
        try {
            ScoringMapperValidateUtil.validateDatatype(datatype, models);
        } catch (LedpException e1) {
            assertTrue(true, "It should pass the validation without throwing any exceptions.");
        }
        
        try {
            ScoringMapperValidateUtil.validateDatatype(null, models);
        } catch (LedpException e1) {
            assertTrue(e1.getCode() == LedpCode.LEDP_20001, "Not providing datatype file cannot pass the validation.");
        }

        
        try {
            ScoringMapperValidateUtil.validateDatatype(datatype, null);
        } catch (LedpException e1) {
            assertTrue(e1.getCode() == LedpCode.LEDP_20001, "Not providing models cannot pass the validation.");
        }

        HashMap<String, JSONObject> fakeModels = new HashMap<String, JSONObject>();
        fakeModels.put("fakeModelID", new JSONObject());
        try {
            ScoringMapperValidateUtil.validateDatatype(datatype, fakeModels);
        } catch (LedpException e1) {
            assertTrue(e1.getCode() == LedpCode.LEDP_20001, "Model not containing proper metadata cannot pass the validation.");
        }

        // datatype can only be 0 or 1
        assertTrue(datatype.containsKey("SemrushRank"), "datatype contains the key 'SemrushRank'");
        datatype.put("SemrushRank", 2l);
        try {
            ScoringMapperValidateUtil.validateDatatype(datatype, models);
        } catch (LedpException e1) {
            assertTrue(e1.getCode() == LedpCode.LEDP_20001, "Containing unknown datatype cannot pass the validation.");
        }
        // change the wrong value back to correct
        datatype.put("SemrushRank", 0l);

        datatype.remove("PercentileModel");
        assertFalse(datatype.containsKey("PercentileModel"), "datatype does not contain the key 'PercentileModel'");
        try {
            ScoringMapperValidateUtil.validateDatatype(datatype, models);
        } catch (LedpException e1) {
            assertTrue(e1.getCode() == LedpCode.LEDP_20001, "Missing required column cannot pass the validation.");
        }

        datatype.put("PercentileModel", 0l);
        assertTrue(datatype.containsKey("PercentileModel"), "datatype contains the key 'PercentileModel'");
        try {
            ScoringMapperValidateUtil.validateDatatype(datatype, models);
        } catch (LedpException e1) {
            assertTrue(e1.getCode() == LedpCode.LEDP_20001,  "Mismatching required column cannot pass the validation.");
        }
    }

}
