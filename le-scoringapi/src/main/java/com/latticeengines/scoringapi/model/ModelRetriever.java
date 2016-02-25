package com.latticeengines.scoringapi.model;

import java.util.List;

import org.springframework.web.bind.annotation.PathVariable;

import com.latticeengines.scoringapi.exposed.Fields;
import com.latticeengines.scoringapi.exposed.Model;
import com.latticeengines.scoringapi.exposed.ModelType;

public interface ModelRetriever {

    List<Model> getActiveModels(String tenantId, ModelType type);

    Fields getModelFields(@PathVariable String modelId);
}
