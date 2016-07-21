package com.latticeengines.scoringapi.exposed.model;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.scoringapi.DataComposition;
import com.latticeengines.domain.exposed.scoringapi.DebugScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.domain.exposed.scoringapi.ScoreResponse;
import com.latticeengines.scoringapi.exposed.InterpretedFields;
import com.latticeengines.scoringapi.exposed.ScoringArtifacts;
import com.latticeengines.scoringapi.exposed.exception.ScoringApiException;

public interface ModelJsonTypeHandler {
    static final String PMML_FILENAME = "rfpmml.xml";
    static final String PMML_MODEL = "PmmlModel";
    static final String HDFS_ENHANCEMENTS_DIR = "enhancements/";
    static final String SCORE_DERIVATION_FILENAME = "scorederivation.json";
    static final String DATA_COMPOSITION_FILENAME = "datacomposition.json";

    boolean accept(String modelJsonType);

    ModelEvaluator getModelEvaluator(String hdfsScoreArtifactBaseDir, String modelJsonType,
            String localPathToPersist);

    ScoreDerivation getScoreDerivation(String hdfsScoreArtifactBaseDir, String modelJsonType,
            String localPathToPersist);

    DataComposition getEventTableDataComposition(String hdfsScoreArtifactTableDir,
            String localPathToPersist);

    DataComposition getDataScienceDataComposition(String hdfsScoreArtifactBaseDir,
            String localPathToPersist);

    ScoreResponse generateScoreResponse(ScoringArtifacts scoringArtifacts,
            Map<String, Object> transformedRecord);

    DebugScoreResponse generateDebugScoreResponse(ScoringArtifacts scoringArtifacts,
            Map<String, Object> transformedRecord, Map<String, Object> matchedRecord);

    ScoringApiException checkForMissingEssentialFields(String recordId, String modelId,
            boolean hasOneOfDomain, boolean hasCompanyName, boolean hasCompanyState,
            List<String> missingMatchFields);

    SimpleEntry<Map<String, Object>, InterpretedFields> parseRecord(String recordId,
            Map<String, FieldSchema> fieldSchemas, Map<String, Object> record, String modelId);
}
