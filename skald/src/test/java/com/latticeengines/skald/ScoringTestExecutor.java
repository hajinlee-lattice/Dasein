package com.latticeengines.skald;

import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ScoringTestExecutor {
    TestDefinition definition;
    public ScoringTestExecutor(TestDefinition definition) {
        this.definition = definition;
    }
    
    @Test(groups = "functional")
    public void run() {
        MockScoreService service = new MockScoreService(definition.modelsPMML, definition.combination);
        
        for (int i = 0; i < definition.records.size(); ++i) {
            Map<String,Object> record = definition.records.get(i);
            Map<ScoreType,Object> expected = definition.scores.get(i);
            Map<ScoreType,Object> actual = service.score(record);
            
            // Iterate over all expected output fields and only assert their equivalence.
            // Additional outputs from the scoring service should be able to be added
            // without causing existing functional tests to fail.
            for (Map.Entry<ScoreType, Object> pair : expected.entrySet()) {
                ScoreType type = pair.getKey();
                Assert.assertTrue(actual.containsKey(type));
                Assert.assertTrue(scoreOutputEquals(type, pair.getValue(), actual.get(type)));
            }
        }
    }
    
    private boolean scoreOutputEquals(ScoreType type, Object expected, Object actual) {
        // TODO consolidate score output semantic and type information - migrate to ScoreType
        if (type == ScoreType.PROBABILITY) {
            Double dexpected = (Double)expected;
            Double dactual = (Double)actual;
            return Math.abs(dexpected.doubleValue() - dactual.doubleValue()) < 1.0e-8;
        }
        else {
            throw new UnsupportedOperationException();
        }
    }
}
