package com.latticeengines.scoringapi.exposed.model.impl;

import java.io.InputStream;
import java.io.Reader;
import java.util.Map;

import org.dmg.pmml.FieldName;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.FieldValue;
import org.jpmml.evaluator.ProbabilityDistribution;

import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.scoringapi.exposed.ScoreType;

public class PMMLModelEvaluator extends DefaultModelEvaluator {

    public PMMLModelEvaluator(InputStream is) {
        super(is);
    }

    public PMMLModelEvaluator(Reader pmml) {
        super(pmml);
    }

    @Override
    protected void calculatePercentile(ScoreDerivation derivation, Map<FieldName, ?> results,
            Map<ScoreType, Object> result) {
        String target = results.keySet().iterator().next().getValue();

        ProbabilityDistribution classification = (ProbabilityDistribution) results.get(new FieldName(target));
        double predicted = classification.getProbability("1");

        result.put(ScoreType.PROBABILITY, predicted);
        result.put(ScoreType.PERCENTILE, new Double(predicted * 100).intValue());
    }

    @Override
    protected boolean shouldThrowExceptionForNullFields() {
        return false;
    }

    @Override
    protected void prepare(Evaluator evaluator, Map<FieldName, FieldValue> arguments, boolean debugRow, FieldName name,
            Object value) {
        try {
            super.prepare(evaluator, arguments, debugRow, name, value);
        } catch (Exception e) {
            super.prepare(evaluator, arguments, debugRow, name, DEFAULT_DOUBLE_VALUE);
        }
    }

    @Override
    protected void inspectEvaluatedResult(Map<FieldName, ?> results) {
        // do nothing for PMML model
    }

    @Override
    protected ProbabilityDistribution getClassification(Map<FieldName, ?> results, String target) {
        ProbabilityDistribution classification = null;
        if (target == null) {
            for (Map.Entry<FieldName, ?> entry : results.entrySet()) {
                if (entry.getValue() instanceof ProbabilityDistribution) {
                    classification = (ProbabilityDistribution) entry.getValue();
                    break;
                }
            }
        } else {
            classification = (ProbabilityDistribution) results.get(new FieldName(target));
        }
        return classification;
    }
}
