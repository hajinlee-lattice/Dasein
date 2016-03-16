package com.latticeengines.scoringapi.exposed;

import java.io.File;

import com.latticeengines.domain.exposed.scoringapi.DataComposition;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.scoringapi.model.ModelEvaluator;

public class ScoringArtifacts {

    private final String modelId;
    private final DataComposition dataScienceDataComposition;
    private final DataComposition eventTableDataComposition;
    private final ScoreDerivation scoreDerivation;
    private final ModelEvaluator pmmlEvaluator;
    private final File modelArtifactsDir;

    public ScoringArtifacts(String modelId, DataComposition dataScienceDataComposition, DataComposition eventTableDataComposition,
            ScoreDerivation scoreDerivation, ModelEvaluator pmmlEvaluator, File modelArtifactsDir) {
        super();
        this.modelId = modelId;
        this.dataScienceDataComposition = dataScienceDataComposition;
        this.eventTableDataComposition = eventTableDataComposition;
        this.scoreDerivation = scoreDerivation;
        this.pmmlEvaluator = pmmlEvaluator;
        this.modelArtifactsDir = modelArtifactsDir;
    }

    public String getModelId() {
        return modelId;
    }

    public DataComposition getDataScienceDataComposition() {
        return dataScienceDataComposition;
    }

    public DataComposition getEventTableDataComposition() {
        return eventTableDataComposition;
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
