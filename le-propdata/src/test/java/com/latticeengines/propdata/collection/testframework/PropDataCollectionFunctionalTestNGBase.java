package com.latticeengines.propdata.collection.testframework;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.propdata.core.source.Source;

public abstract class PropDataCollectionFunctionalTestNGBase extends PropDataCollectionAbstractTestNGBase {

    protected static final String progressCreator = "FunctionalTest";

    protected void prepareCleanPod(Source source) {
        String podId = "Functional" + source.getSourceName();
        hdfsPathBuilder.changeHdfsPodId(podId);
        try {
            HdfsUtils.rmdir(yarnConfiguration, hdfsPathBuilder.podDir().toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
