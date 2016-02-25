package com.latticeengines.scoringapi.exposed;

import com.latticeengines.domain.exposed.scoringapi.DataComposition;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.scoringapi.model.ModelEvaluator;

public class ModelArtifacts {

//    private model id
    private DataComposition dataComposition;
    private ScoreDerivation scoreDerivation;
    private ModelEvaluator pmmlEvaluator;

    ModelArtifacts(DataComposition dataComposition, ScoreDerivation scoreDerivation, ModelEvaluator pmmlEvaluator) {
        super();
        this.dataComposition = dataComposition;
        this.scoreDerivation = scoreDerivation;
        this.pmmlEvaluator = pmmlEvaluator;
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
