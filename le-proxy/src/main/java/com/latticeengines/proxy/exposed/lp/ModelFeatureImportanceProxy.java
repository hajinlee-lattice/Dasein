package com.latticeengines.proxy.exposed.lp;

import java.util.List;

import com.latticeengines.domain.exposed.pls.ModelFeatureImportance;

public interface ModelFeatureImportanceProxy {

    void upsertModelFeatureImportances(String customerSpace, String modelGuId);

    List<ModelFeatureImportance> getFeatureImportanceByModelGuid(String customerSpace, String modelGuid);

}
