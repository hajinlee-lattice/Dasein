package com.latticeengines.scoringapi.exposed;

import java.io.File;

import com.latticeengines.domain.exposed.scoringapi.DataComposition;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.scoringapi.model.ModelEvaluator;

public class ScoringArtifacts {

    private final String modelId;
    private final DataComposition dataComposition;
    private final ScoreDerivation scoreDerivation;
    private final ModelEvaluator pmmlEvaluator;
    private final File modelArtifactsDir;

    public ScoringArtifacts(String modelId, DataComposition dataComposition, ScoreDerivation scoreDerivation,
            ModelEvaluator pmmlEvaluator, File modelArtifactsDir) {
        super();
        this.modelId = modelId;
        this.dataComposition = dataComposition;
        this.scoreDerivation = scoreDerivation;
        this.pmmlEvaluator = pmmlEvaluator;
        this.modelArtifactsDir = modelArtifactsDir;
    }

    public String getModelId() {
        return modelId;
    }

    public DataComposition getDataComposition() {
        return dataComposition;
    }

    public ScoreDerivation getScoreDerivation() {
        return scoreDerivation;
    }

    public ModelEvaluator getPmmlEvaluator() {
        return pmmlEvaluator;
    }

    public File getModelArtifactsDir() {
        return modelArtifactsDir;
    }

}
