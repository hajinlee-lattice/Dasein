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
                Assert.assertTrue(ScoreType.valueEquals(type, pair.getValue(), actual.get(type)));
            }
        }
    }
}
