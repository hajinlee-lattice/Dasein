package com.latticeengines.skald;

import java.util.HashMap;
import java.util.Map;

import org.springframework.stereotype.Service;

import com.latticeengines.skald.model.PredictiveModel;
import com.latticeengines.skald.model.ScoreDerivation;

@Service
public class ModelEvaluator {
    public Map<String, Object> evaluate(PredictiveModel model, ScoreDerivation derivation, Map<String, Object> record) {
        // TODO: Score the PMML model.

        // TODO: Create derived score elements.

        Map<String, Object> result = new HashMap<String, Object>();
        result.put("probability", 0.825);
        result.put("lift", 3.5);
        result.put("percentile", 96);
        result.put("bucket", "A");
        result.put("fake", true);
        return result;
    }
}
