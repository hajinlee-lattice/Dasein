package com.latticeengines.scoringapi.exposed;

import java.io.File;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.scoringapi.DataComposition;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.scoringapi.ModelType;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.scoringapi.exposed.model.ModelEvaluator;

public class ScoringArtifacts {

    private ModelSummary modelSummary;
    private final ModelType modelType;
    private final DataComposition dataScienceDataComposition;
    private final DataComposition eventTableDataComposition;
    private final ScoreDerivation scoreDerivation;
    private final ModelEvaluator pmmlEvaluator;
    private final File modelArtifactsDir;
    private final Map<String, FieldSchema> fieldSchemas;
    private final String modelJsonType;
    private List<BucketMetadata> bucketMetadataList;

    public ScoringArtifacts(ModelSummary modelSummary, //
            ModelType modelType, DataComposition dataScienceDataComposition, //
            DataComposition eventTableDataComposition, ScoreDerivation scoreDerivation, //
            ModelEvaluator pmmlEvaluator, File modelArtifactsDir, //
            Map<String, FieldSchema> fieldSchemas, String modelJsonType, //
            List<BucketMetadata> bucketMetadataList) {
        super();
        this.modelSummary = modelSummary;
        this.modelType = modelType;
        this.dataScienceDataComposition = dataScienceDataComposition;
        this.eventTableDataComposition = eventTableDataComposition;
        this.scoreDerivation = scoreDerivation;
        this.pmmlEvaluator = pmmlEvaluator;
        this.modelArtifactsDir = modelArtifactsDir;
        this.fieldSchemas = fieldSchemas;
        this.modelJsonType = modelJsonType;
        this.bucketMetadataList = bucketMetadataList;
    }

    public ModelSummary getModelSummary() {
        return modelSummary;
    }

    public ModelType getModelType() {
        return modelType;
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

    public Map<String, FieldSchema> getFieldSchemas() {
        return fieldSchemas;
    }

    public String getModelJsonType() {
        return modelJsonType;
    }

    public List<BucketMetadata> getBucketMetadataList() {
        return bucketMetadataList;
    }

    public void setModelSummary(ModelSummary modelSummary) {
        this.modelSummary = modelSummary;
    }

    public void setBucketMetadataList(List<BucketMetadata> bucketMetadataList) {
        this.bucketMetadataList = bucketMetadataList;
    }

}
