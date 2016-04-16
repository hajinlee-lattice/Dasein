package com.latticeengines.propdata.engine.transformation.service;

import com.latticeengines.domain.exposed.propdata.manage.TransformationProgress;

public interface TransformationExecutor {

    void kickOffNewProgress();

    void proceedProgress(TransformationProgress progress);

    void purgeOldVersions();

}
