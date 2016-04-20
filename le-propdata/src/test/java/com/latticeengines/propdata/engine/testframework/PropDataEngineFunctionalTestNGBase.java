package com.latticeengines.propdata.engine.testframework;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.propdata.core.service.impl.HdfsPodContext;
import com.latticeengines.propdata.core.source.Source;

public abstract class PropDataEngineFunctionalTestNGBase extends PropDataEngineAbstractTestNGBase {

    protected static final String progressCreator = "FunctionalTest";

    protected void prepareCleanPod(Source source) {
        String podId = "Functional" + source.getSourceName();
        prepareCleanPod(podId);
    }

    protected void prepareCleanPod(String podId) {
        HdfsPodContext.changeHdfsPodId(podId);
        try {
            HdfsUtils.rmdir(yarnConfiguration, hdfsPathBuilder.podDir().toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
