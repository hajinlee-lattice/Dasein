package com.latticeengines.propdata.collection.testframework;

import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.proxy.exposed.propdata.SqlProxy;

public abstract class PropDataCollectionFunctionalTestNGBase extends PropDataCollectionAbstractTestNGBase {

    protected static final String progressCreator = "FunctionalTest";

    @Autowired
    protected SqlProxy sqlProxy;

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
