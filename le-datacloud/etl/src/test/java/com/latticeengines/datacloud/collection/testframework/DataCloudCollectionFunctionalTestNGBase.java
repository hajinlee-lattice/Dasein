package com.latticeengines.datacloud.collection.testframework;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.util.HdfsPodContext;

public abstract class DataCloudCollectionFunctionalTestNGBase extends DataCloudCollectionAbstractTestNGBase {

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
