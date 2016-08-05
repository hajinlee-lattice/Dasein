package com.latticeengines.modelquality.service;

import com.latticeengines.domain.exposed.modelquality.Environment;
import com.latticeengines.domain.exposed.modelquality.ModelRun;

public interface ModelRunService {

    String run(ModelRun modelRun, Environment env);

    void setEnvironment(Environment env);

}
