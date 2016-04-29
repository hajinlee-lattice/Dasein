package com.latticeengines.propdata.engine.testframework;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.propdata.core.service.impl.HdfsPodContext;

public abstract class PropDataEngineDeploymentTestNGBase extends PropDataEngineAbstractTestNGBase {

    protected void prepareCleanPod(String podId) {
        HdfsPodContext.changeHdfsPodId(podId);
        try {
            HdfsUtils.rmdir(yarnConfiguration, hdfsPathBuilder.podDir().toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
