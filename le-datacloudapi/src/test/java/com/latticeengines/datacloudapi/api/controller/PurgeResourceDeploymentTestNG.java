package com.latticeengines.datacloudapi.api.controller;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloudapi.api.testframework.PropDataApiDeploymentTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeSource;
import com.latticeengines.proxy.exposed.datacloudapi.PurgeProxy;

public class PurgeResourceDeploymentTestNG extends PropDataApiDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(PurgeResourceDeploymentTestNG.class);

    public final String POD_ID = this.getClass().getSimpleName();

    @Autowired
    private PurgeProxy purgeProxy;

    /**
     * Just a simple test to see if mocked API works fine
     */
    @Test(groups = "deployment")
    public void testGetPurgeSources() {
        List<PurgeSource> list = purgeProxy.getPurgeSources(POD_ID);
        for (PurgeSource src : list) {
            log.info(JsonUtils.serialize(src));
        }
    }
}
