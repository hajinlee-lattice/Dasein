package com.latticeengines.scoringapi.exposed.model;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.scoringapi.DataComposition;
import com.latticeengines.domain.exposed.scoringapi.DebugScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.domain.exposed.scoringapi.ScoreResponse;
import com.latticeengines.scoringapi.exposed.ScoringArtifacts;

public interface ModelJsonTypeHandler {
    static final String PMML_FILENAME = "rfpmml.xml";
    static final String PMML_MODEL = "PmmlModel";
    static final String HDFS_ENHANCEMENTS_DIR = "enhancements/";
    static final String SCORE_DERIVATION_FILENAME = "scorederivation.json";
    static final String DATA_COMPOSITION_FILENAME = "datacomposition.json";

    boolean accept(String modelJsonType);

    ModelEvaluator getModelEvaluator(String hdfsScoreArtifactBaseDir, String modelJsonType, String localPathToPersist);

    ScoreDerivation getScoreDerivation(String hdfsScoreArtifactBaseDir, String modelJsonType,
            String localPathToPersist);

    DataComposition getEventTableDataComposition(String hdfsScoreArtifactTableDir, String localPathToPersist);

    DataComposition getDataScienceDataComposition(String hdfsScoreArtifactBaseDir, String localPathToPersist);

    ScoreResponse generateScoreResponse(ScoringArtifacts scoringArtifacts, Map<String, Object> transformedRecord);

    DebugScoreResponse generateDebugScoreResponse(ScoringArtifacts scoringArtifacts,
            Map<String, Object> transformedRecord, Map<String, Object> matchedRecord);

    void checkForMissingEssentialFields(boolean hasOneOfDomain, boolean hasCompanyName, boolean hasCompanyState,
            List<String> missingMatchFields);
}
