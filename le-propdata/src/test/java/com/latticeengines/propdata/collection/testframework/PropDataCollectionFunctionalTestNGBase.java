package com.latticeengines.propdata.collection.testframework;

import com.latticeengines.common.exposed.util.HdfsUtils;

public abstract class PropDataCollectionFunctionalTestNGBase extends PropDataCollectionAbstractTestNGBase {

    protected static final String progressCreator = "FunctionalTest";

    protected void prepareCleanPod(String podId) {
        hdfsPathBuilder.changeHdfsPodId(podId);
        try {
            HdfsUtils.rmdir(yarnConfiguration, hdfsPathBuilder.podDir().toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
