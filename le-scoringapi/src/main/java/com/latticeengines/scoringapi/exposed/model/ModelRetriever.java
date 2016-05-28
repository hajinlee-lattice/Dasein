package com.latticeengines.scoringapi.exposed.model;

import java.util.List;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.scoringapi.Fields;
import com.latticeengines.domain.exposed.scoringapi.Model;
import com.latticeengines.domain.exposed.scoringapi.ModelDetail;
import com.latticeengines.domain.exposed.scoringapi.ModelType;
import com.latticeengines.scoringapi.exposed.ScoreCorrectnessArtifacts;
import com.latticeengines.scoringapi.exposed.ScoringArtifacts;

public interface ModelRetriever {

    List<Model> getActiveModels(CustomerSpace customerSpace, ModelType type);

    Fields getModelFields(CustomerSpace customerSpace, String modelId);

    ScoringArtifacts getModelArtifacts(CustomerSpace customerSpace, String modelId);

    ScoringArtifacts retrieveModelArtifactsFromHdfs(CustomerSpace customerSpace, String modelId);

    void setLocalPathToPersist(String localPathToPersist);

    ScoreCorrectnessArtifacts retrieveScoreCorrectnessArtifactsFromHdfs(CustomerSpace customerSpace, String modelId);

    int getModelsCount(CustomerSpace customerSpace, String start, boolean considerAllStatus);

    List<ModelDetail> getPaginatedModels(CustomerSpace customerSpace, String start, int offset, int maximum,
            boolean considerAllStatus);
}
