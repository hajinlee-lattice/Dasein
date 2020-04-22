package com.latticeengines.apps.lp.service;

import java.util.List;

import com.latticeengines.domain.exposed.pls.ModelFeatureImportance;

public interface ModelFeatureImportanceService {

    void upsertFeatureImportances(String customerSpace, String modelId);

    List<ModelFeatureImportance> getFeatureImportances(String customerSpace, String modelGuid);

}
