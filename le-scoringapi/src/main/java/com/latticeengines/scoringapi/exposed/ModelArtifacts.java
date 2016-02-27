package com.latticeengines.scoringapi.exposed;

import com.latticeengines.domain.exposed.scoringapi.DataComposition;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.scoringapi.model.ModelEvaluator;

public class ModelArtifacts {

    private String modelId;
    private DataComposition dataComposition;
    private ScoreDerivation scoreDerivation;
    private ModelEvaluator pmmlEvaluator;

    public ModelArtifacts(String modelId, DataComposition dataComposition, ScoreDerivation scoreDerivation, ModelEvaluator pmmlEvaluator) {
        super();
        this.modelId = modelId;
        this.dataComposition = dataComposition;
        this.scoreDerivation = scoreDerivation;
        this.pmmlEvaluator = pmmlEvaluator;
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

}
