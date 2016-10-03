package com.latticeengines.modelquality.service;

import com.latticeengines.domain.exposed.modelquality.Environment;
import com.latticeengines.domain.exposed.modelquality.ModelRun;
import com.latticeengines.domain.exposed.modelquality.ModelRunEntityNames;

public interface ModelRunService {

    ModelRun createModelRun(ModelRunEntityNames modelRunEntityNames, Environment env);

    void setEnvironment(Environment env);

}
