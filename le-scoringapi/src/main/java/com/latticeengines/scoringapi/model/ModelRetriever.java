package com.latticeengines.scoringapi.model;

import java.util.List;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.scoringapi.exposed.Fields;
import com.latticeengines.scoringapi.exposed.Model;
import com.latticeengines.scoringapi.exposed.ModelArtifacts;
import com.latticeengines.scoringapi.exposed.ModelType;

public interface ModelRetriever {

    List<Model> getActiveModels(CustomerSpace customerSpace, ModelType type);

    Fields getModelFields(CustomerSpace customerSpace, String modelId);

    ModelArtifacts getModelArtifacts(CustomerSpace customerSpace, String modelId);
}
