package com.latticeengines.network.exposed.modelquality;

import java.util.List;

import com.latticeengines.domain.exposed.modelquality.ModelRunEntityNames;

public interface ModelQualityModelRunInterface {

    List<ModelRunEntityNames> getModelRuns();

    String createModelRun(ModelRunEntityNames modelRunEntityNames, String tenant, String username, String password,
            String apiHostPort);

    ModelRunEntityNames getModelRunByName(String modelRunName);

    String getModelRunStatusByName(String modelRunName);

    String getModelRunModelHDFSDirByName(String modelRunName);

}
