package com.latticeengines.propdata.engine.transformation.service;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.propdata.engine.transformation.entitymgr.TransformationProgressEntityMgr;

public interface TransformationExecutor {

    TransformationProgress kickOffNewProgress(TransformationProgressEntityMgr progressEntityMgr,
            List<String> baseVersions, String targetVersion);

    void purgeOldVersions();

}
