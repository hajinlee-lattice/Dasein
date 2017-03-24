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
        PMMLModelEvaluator d = new PMMLModelEvaluator(Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream("com/latticeengines/scoringapi/pmmlmodels/numeric_mapping_test.xml.fixed.xml"));
        Map<String, Object> arguments = new HashMap<>();
        arguments.put("x", new Double(5.0));
        arguments.put("y", new Double(5.0));
        arguments.put("z", new Double(5.0));
        arguments.put("label", "5.0");
        Map<ScoreType, Object> evaluation = d.evaluate(arguments, null);
        Integer i = (Integer) evaluation.get(ScoreType.PERCENTILE);
        Object c = evaluation.get(ScoreType.CLASSIFICATION);
        assertEquals(i.intValue(), 6);
        assertEquals(c.toString(), "0");

        arguments = new HashMap<>();
        arguments.put("x", new Double(1.0));
        arguments.put("y", new Double(1.0));
        arguments.put("z", new Double(1.0));
        arguments.put("label", "1.0");
        evaluation = d.evaluate(arguments, null);
        i = (Integer) evaluation.get(ScoreType.PERCENTILE);
        c = evaluation.get(ScoreType.CLASSIFICATION);
        assertEquals(i.intValue(), 0);
        assertEquals(c.toString(), "0");

        arguments = new HashMap<>();
        arguments.put("x", new Double(10.0));
        arguments.put("y", new Double(10.0));
        arguments.put("z", new Double(10.0));
        arguments.put("label", "10.0");
        evaluation = d.evaluate(arguments, null);
        i = (Integer) evaluation.get(ScoreType.PERCENTILE);
        c = evaluation.get(ScoreType.CLASSIFICATION);
        assertEquals(i.intValue(), 100);
        assertEquals(c.toString(), "1");
    }

    @Test(groups = "functional")
    public void testProcessNNIris() throws FileNotFoundException {
        PMMLModelEvaluator d = new PMMLModelEvaluator(
                Thread.currentThread().getContextClassLoader() //
                        .getResourceAsStream("com/latticeengines/scoringapi/pmmlmodels/nn_iris.xml"));
        Map<String, Object> arguments = new HashMap<>();
        arguments.put("sepal_length", new Double(0));
        arguments.put("sepal_width", new Double(10));
        arguments.put("petal_length", new Double(1));
        arguments.put("petal_width", new Double(5));
        Map<ScoreType, Object> evaluation = d.evaluate(arguments, null);
        System.out.println(evaluation);
        Integer i = (Integer) evaluation.get(ScoreType.PERCENTILE);
        Object c = evaluation.get(ScoreType.CLASSIFICATION);
        assertEquals(i.intValue(), 93);
        assertEquals(c.toString(), "Iris-versicolor");
    }
}
