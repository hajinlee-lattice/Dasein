package com.latticeengines.propdata.engine.transformation.service;

import com.latticeengines.domain.exposed.propdata.manage.TransformationProgress;
import com.latticeengines.propdata.engine.transformation.entitymgr.TransformationProgressEntityMgr;

public interface TransformationExecutor {

    TransformationProgress kickOffNewProgress(TransformationProgressEntityMgr transformationProgressEntityMgr);

    void purgeOldVersions();

}
