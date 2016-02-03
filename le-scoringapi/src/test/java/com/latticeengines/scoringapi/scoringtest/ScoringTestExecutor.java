package com.latticeengines.scoringapi.scoringtest;

import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.scoringapi.unused.ScoreType;

public class ScoringTestExecutor {
    TestDefinition definition;

    public ScoringTestExecutor(TestDefinition definition) {
        this.definition = definition;
    }

    @Test(groups = "functional", enabled=false)
    public void run() {
        MockScoreService service = new MockScoreService(definition.modelsPMML, definition.combination);

        for (int i = 0; i < definition.records.size(); ++i) {
            Map<String, Object> record = definition.records.get(i);
            Map<ScoreType, Object> expected = definition.scores.get(i);
            Map<ScoreType, Object> actual = service.score(record);

            // Iterate over all expected output fields and only assert their
            // equivalence.
            // Additional outputs from the scoring service should be able to be
            // added
            // without causing existing functional tests to fail.
            for (Map.Entry<ScoreType, Object> pair : expected.entrySet()) {
                ScoreType type = pair.getKey();
                Assert.assertTrue(actual.containsKey(type), "Record " + i + " missing value " + type.toString());
                Assert.assertTrue(valueEquals(type, pair.getValue(), actual.get(type)), "Record " + i + " value "
                        + type.toString() + " was " + actual.get(type) + " not " + pair.getValue());
            }
        }

    }

    private static boolean valueEquals(ScoreType scoretype, Object value1, Object value2) {
        Class<?> clazz = scoretype.type();
        if (clazz == Double.class) {
            return Math.abs(((Double) value1).doubleValue() - ((Double) value2).doubleValue()) < 1e-10;
        }
        return value1.equals(value2);
    }
}
