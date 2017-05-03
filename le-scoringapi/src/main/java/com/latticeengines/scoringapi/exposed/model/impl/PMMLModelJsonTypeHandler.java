package com.latticeengines.scoringapi.exposed.model.impl;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.scoringapi.FieldType;
import com.latticeengines.domain.exposed.scoringapi.ScoreResponse;
import com.latticeengines.scoringapi.exposed.ScoreEvaluation;
import com.latticeengines.scoringapi.exposed.ScoreType;
import com.latticeengines.scoringapi.exposed.ScoringArtifacts;
import com.latticeengines.scoringapi.exposed.exception.ScoringApiException;
import com.latticeengines.scoringapi.exposed.model.ModelEvaluator;

@Component
public class PMMLModelJsonTypeHandler extends DefaultModelJsonTypeHandler {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(DefaultModelJsonTypeHandler.class);

    @Override
    public boolean accept(String modelJsonType) {
        return PMML_MODEL.equals(modelJsonType);
    }

    @Override
    protected boolean shouldStopCheckForScoreDerivation(String path) throws IOException {
        // for PmmlModel then relax condition for score derivation file
        if (!HdfsUtils.fileExists(yarnConfiguration, path)) {
            return true;
        }
        return false;
    }

    @Override
    protected ModelEvaluator initModelEvaluator(FSDataInputStream is) {
        return new PMMLModelEvaluator(is);
    }

    @Override
    public ScoringApiException checkForMissingEssentialFields(String recordId, //
            String modelId, //
            boolean hasOneOfDomain, //
            boolean hasCompanyName, //
            List<String> missingMatchFields) {
        return null;
    }

    @Override
    protected boolean shouldThrowExceptionForMismatchedDataTypes() {
        return false;
    }

    @Override
    protected void handleException(Map<String, AbstractMap.SimpleEntry<Class<?>, Object>> mismatchedDataTypes,
            String fieldName, Object fieldValue, FieldType fieldType, Map<String, Object> record) {
        // just put field and original value for PMML model case
        record.put(fieldName, fieldValue);
    }

    @Override
    public ScoreResponse generateScoreResponse(ScoringArtifacts scoringArtifacts, //
            Map<String, Object> transformedRecord) {
        ScoreResponse scoreResponse = new ScoreResponse();
        ScoreEvaluation scoreEvaluation = score(scoringArtifacts, transformedRecord);
        scoreResponse.setClassification(scoreEvaluation.getClassification());
        scoreResponse.setScore(scoreEvaluation.getPercentile());
        return scoreResponse;
    }

    @Override
    protected ScoreEvaluation score(ScoringArtifacts scoringArtifacts, //
            Map<String, Object> transformedRecord) {
        Map<ScoreType, Object> evaluation = scoringArtifacts.getPmmlEvaluator().evaluate(transformedRecord,
                scoringArtifacts.getScoreDerivation());

        Double p = (Double) evaluation.get(ScoreType.PROBABILITY_OR_VALUE);
        Integer i = (Integer) evaluation.get(ScoreType.PERCENTILE);

        ScoreEvaluation scoreEvaluation = null;

        if (i != null) {
            scoreEvaluation = new ScoreEvaluation(-1.0, i);
            scoreEvaluation.setScoreType(ScoreType.PERCENTILE);
        } else if (p != null) {
            double probability = BigDecimal.valueOf(p).setScale(8, RoundingMode.HALF_UP).doubleValue();
            scoreEvaluation = new ScoreEvaluation(probability, -1);
            scoreEvaluation.setScoreType(ScoreType.PROBABILITY_OR_VALUE);
        }

        Object c = evaluation.get(ScoreType.CLASSIFICATION);
        String classification = String.valueOf(c);
        if (c == null) {
            classification = null;
        }

        scoreEvaluation.setClassification(classification);

        return scoreEvaluation;
    }

}
