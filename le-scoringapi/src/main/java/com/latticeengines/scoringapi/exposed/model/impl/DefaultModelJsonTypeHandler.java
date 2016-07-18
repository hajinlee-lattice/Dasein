package com.latticeengines.scoringapi.exposed.model.impl;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.base.Joiner;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.scoringapi.DataComposition;
import com.latticeengines.domain.exposed.scoringapi.DebugScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.domain.exposed.scoringapi.ScoreResponse;
import com.latticeengines.scoringapi.exposed.ScoreEvaluation;
import com.latticeengines.scoringapi.exposed.ScoreType;
import com.latticeengines.scoringapi.exposed.ScoringArtifacts;
import com.latticeengines.scoringapi.exposed.exception.ScoringApiException;
import com.latticeengines.scoringapi.exposed.model.ModelEvaluator;
import com.latticeengines.scoringapi.exposed.model.ModelJsonTypeHandler;

@Component
public class DefaultModelJsonTypeHandler implements ModelJsonTypeHandler {
    private static final Log log = LogFactory.getLog(DefaultModelJsonTypeHandler.class);

    @Autowired
    protected Configuration yarnConfiguration;

    @Override
    public boolean accept(String modelJsonType) {
        // anything other than PmmlModel, it future it will change if more types
        // are checked
        return !PMML_MODEL.equals(modelJsonType);
    }

    @Override
    public ModelEvaluator getModelEvaluator(String hdfsScoreArtifactBaseDir, //
            String modelJsonType, //
            String localPathToPersist) {
        FSDataInputStream is = null;
        String path = hdfsScoreArtifactBaseDir + PMML_FILENAME;

        ModelEvaluator modelEvaluator = null;
        try {
            FileSystem fs = FileSystem.newInstance(yarnConfiguration);
            is = fs.open(new Path(path));

            modelEvaluator = initModelEvaluator(is);

            if (!StringUtils.isBlank(localPathToPersist)) {
                HdfsUtils.copyHdfsToLocal(yarnConfiguration, path, localPathToPersist + PMML_FILENAME);
            }
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_31000, new String[] { path });
        }
        return modelEvaluator;
    }

    @Override
    public ScoreDerivation getScoreDerivation(String hdfsScoreArtifactBaseDir, //
            String modelJsonType, //
            String localPathToPersist) {
        String path = hdfsScoreArtifactBaseDir + HDFS_ENHANCEMENTS_DIR + SCORE_DERIVATION_FILENAME;
        String content = null;
        try {
            if (shouldStopCheckForScoreDerivation(path)) {
                return null;
            }

            content = HdfsUtils.getHdfsFileContents(yarnConfiguration, path);
            if (!StringUtils.isBlank(localPathToPersist)) {
                HdfsUtils.copyHdfsToLocal(yarnConfiguration, path, localPathToPersist + SCORE_DERIVATION_FILENAME);
            }
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_31000, new String[] { path });
        }
        ScoreDerivation scoreDerivation = JsonUtils.deserialize(content, ScoreDerivation.class);
        return scoreDerivation;
    }

    @Override
    public DataComposition getDataScienceDataComposition(String hdfsScoreArtifactBaseDir, //
            String localPathToPersist) {
        String path = hdfsScoreArtifactBaseDir + HDFS_ENHANCEMENTS_DIR + DATA_COMPOSITION_FILENAME;
        String content = null;
        try {
            content = HdfsUtils.getHdfsFileContents(yarnConfiguration, path);
            if (!StringUtils.isBlank(localPathToPersist)) {
                HdfsUtils.copyHdfsToLocal(yarnConfiguration, path, localPathToPersist + DATA_COMPOSITION_FILENAME);
            }
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_31000, new String[] { path });
        }
        DataComposition dataComposition = JsonUtils.deserialize(content, DataComposition.class);
        return dataComposition;
    }

    @Override
    public DataComposition getEventTableDataComposition(String hdfsScoreArtifactTableDir, //
            String localPathToPersist) {
        String path = hdfsScoreArtifactTableDir + DATA_COMPOSITION_FILENAME;
        String content = null;
        try {
            content = HdfsUtils.getHdfsFileContents(yarnConfiguration, path);
            if (!StringUtils.isBlank(localPathToPersist)) {
                HdfsUtils.copyHdfsToLocal(yarnConfiguration, path,
                        localPathToPersist + "metadata-" + DATA_COMPOSITION_FILENAME);
            }
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_31000, new String[] { path });
        }
        DataComposition dataComposition = JsonUtils.deserialize(content, DataComposition.class);
        return dataComposition;
    }

    @Override
    public ScoreResponse generateScoreResponse(ScoringArtifacts scoringArtifacts, //
            Map<String, Object> transformedRecord) {
        ScoreResponse scoreResponse = new ScoreResponse();
        int percentile = score(scoringArtifacts, transformedRecord).getPercentile();
        scoreResponse.setScore(percentile);
        return scoreResponse;
    }

    @Override
    public DebugScoreResponse generateDebugScoreResponse(ScoringArtifacts scoringArtifacts, //
            Map<String, Object> transformedRecord, //
            Map<String, Object> matchedRecord) {
        DebugScoreResponse debugScoreResponse = new DebugScoreResponse();
        ScoreEvaluation scoreEvaluation = score(scoringArtifacts, transformedRecord);
        debugScoreResponse.setProbability(scoreEvaluation.getProbability());
        debugScoreResponse.setScore(scoreEvaluation.getPercentile());
        debugScoreResponse.setTransformedRecord(transformedRecord);
        debugScoreResponse.setMatchedRecord(matchedRecord);

        return debugScoreResponse;
    }

    @Override
    public ScoringApiException checkForMissingEssentialFields(String recordId, //
            String modelId, //
            boolean hasOneOfDomain, //
            boolean hasCompanyName, //
            boolean hasCompanyState, //
            List<String> missingMatchFields) {
        if (!hasOneOfDomain && (!hasCompanyName || !hasCompanyState)) {
            return new ScoringApiException(LedpCode.LEDP_31199,
                    new String[] { Joiner.on(",").join(missingMatchFields) });
        }

        return null;
    }

    protected boolean shouldStopCheckForScoreDerivation(String path) throws IOException {
        return false;
    }

    protected ModelEvaluator initModelEvaluator(FSDataInputStream is) {
        return new DefaultModelEvaluator(is);
    }

    private ScoreEvaluation score(ScoringArtifacts scoringArtifacts, //
            Map<String, Object> transformedRecord) {
        Map<ScoreType, Object> evaluation = scoringArtifacts.getPmmlEvaluator().evaluate(transformedRecord,
                scoringArtifacts.getScoreDerivation());
        double probability = (double) evaluation.get(ScoreType.PROBABILITY);
        Object percentileObject = evaluation.get(ScoreType.PERCENTILE);

        int percentile = (int) percentileObject;
        if (percentile > 99 || percentile < 5) {
            log.warn(String.format("Score out of range; percentile: %d probability: %,.7f", percentile,
                    (double) evaluation.get(ScoreType.PROBABILITY)));
            percentile = Math.min(percentile, 99);
            percentile = Math.max(percentile, 5);
        }

        return new ScoreEvaluation(probability, percentile);
    }
}
