package com.latticeengines.scoringapi.model;

import java.util.List;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.scoringapi.exposed.Fields;
import com.latticeengines.scoringapi.exposed.Model;
import com.latticeengines.scoringapi.exposed.ModelType;
import com.latticeengines.scoringapi.exposed.ScoringArtifacts;

public interface ModelRetriever {

    List<Model> getActiveModels(CustomerSpace customerSpace, ModelType type);

    Fields getModelFields(CustomerSpace customerSpace, String modelId);

    ScoringArtifacts getModelArtifacts(CustomerSpace customerSpace, String modelId);

    ScoringArtifacts retrieveModelArtifactsFromHdfs(CustomerSpace customerSpace, String modelId);

    void setLocalPathToPersist(String localPathToPersist);
}
