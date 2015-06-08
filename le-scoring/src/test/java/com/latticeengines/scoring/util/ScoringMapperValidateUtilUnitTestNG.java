package com.latticeengines.scoring.util;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;

import java.net.URL;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.json.simple.JSONObject;
import org.testng.annotations.Test;

public class ScoringMapperValidateUtilUnitTestNG {

    @Test(groups = "unit")
    public void testProcessScoreFiles() {
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

        ValidationResult vr = ScoringMapperValidateUtil.validate(datatype, models);
        assertTrue(vr.passValidation(), "It should pass the validation");

        vr = ScoringMapperValidateUtil.validate(null, models);
        System.out.println(vr);
        assertFalse(vr.passValidation(), "Not providing datatype file cannot pass the validation.");

        vr = ScoringMapperValidateUtil.validate(datatype, null);
        System.out.println(vr);
        assertFalse(vr.passValidation(), "Not providing models cannot pass the validation.");

        HashMap<String, JSONObject> fakeModels = new HashMap<String, JSONObject>();
        fakeModels.put("fakeModelID", new JSONObject());
        vr = ScoringMapperValidateUtil.validate(datatype, fakeModels);
        System.out.println(vr);
        assertFalse(vr.passValidation(), "Model not containing proper metadata cannot pass the validation.");

        // datatype can only be 0 or 1
        assertTrue(datatype.containsKey("SemrushRank"), "datatype contains the key 'SemrushRank'");
        datatype.put("SemrushRank", 2l);
        vr = ScoringMapperValidateUtil.validate(datatype, models);
        assertFalse(vr.passValidation(), "Containing unknown datatype cannot pass the validation.");
        System.out.println(vr);
        // change the wrong value back to correct
        datatype.put("SemrushRank", 0l);

        datatype.remove("PercentileModel");
        assertFalse(datatype.containsKey("PercentileModel"), "datatype does not contain the key 'PercentileModel'");
        vr = ScoringMapperValidateUtil.validate(datatype, models);
        assertFalse(vr.passValidation(), "Missing required column cannot pass the validation.");
        System.out.println(vr);

        datatype.put("PercentileModel", 0l);
        assertTrue(datatype.containsKey("PercentileModel"), "datatype contains the key 'PercentileModel'");
        vr = ScoringMapperValidateUtil.validate(datatype, models);
        assertFalse(vr.passValidation(), "Mismatching required column cannot pass the validation.");
        System.out.println(vr);
    }

}
