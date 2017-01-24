package com.latticeengines.scoringapi.score.impl;

import static org.testng.Assert.assertEquals;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;

import com.latticeengines.scoringapi.exposed.ScoreType;
import com.latticeengines.scoringapi.exposed.model.impl.PMMLModelEvaluator;
import com.latticeengines.scoringapi.functionalframework.ScoringApiFunctionalTestNGBase;

public class PmmlModelHandlerFunctionalTestNG extends ScoringApiFunctionalTestNGBase {

    @Test(groups = "functional")
    public void testProcess() throws FileNotFoundException {
        PMMLModelEvaluator d = new PMMLModelEvaluator(
                ClassLoader.getSystemResourceAsStream("com/latticeengines/scoringapi/numeric_mapping_test.xml.fixed.xml"));
        Map<String, Object> arguments = new HashMap<>();
        arguments.put("x", new Double(5.0));
        arguments.put("y", new Double(5.0));
        arguments.put("z", new Double(5.0));
        arguments.put("label", "5.0");
        Map<ScoreType, Object> evaluation = d.evaluate(arguments, null);
        Double p = (Double) evaluation.get(ScoreType.PROBABILITY_OR_VALUE);
        Integer i = (Integer) evaluation.get(ScoreType.PERCENTILE);
        Object c = evaluation.get(ScoreType.CLASSIFICATION);
        assertEquals(p, null);
        assertEquals(i.intValue(), 6);
        assertEquals(c.toString(), "0");
    }
}
