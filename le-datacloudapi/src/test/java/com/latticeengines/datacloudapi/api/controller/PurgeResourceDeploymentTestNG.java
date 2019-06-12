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

// dpltc deploy -a datacloudapi
public class PurgeResourceDeploymentTestNG extends PropDataApiDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(PurgeResourceDeploymentTestNG.class);

    public final String POD_ID = this.getClass().getSimpleName();

    @Autowired
    private PurgeProxy purgeProxy;

    /**
     * Just a simple test to see if API works fine
     * Complete test scenarios are covered in functional tests
     */
    @Test(groups = "deployment")
    public void testGetPurgeSources() {
        List<PurgeSource> list = purgeProxy.getPurgeSources(null);
        list.forEach(s -> {
            log.info(JsonUtils.serialize(s));
        });
        list = purgeProxy.getPurgeSources(POD_ID); // Test empty pod
        list.forEach(s -> {
            log.info(JsonUtils.serialize(s));
        });
    }

    @Test(groups = "deployment")
    public void testGetUnknownSources() {
        List<String> list = purgeProxy.getUnknownSources(null);
        list.forEach(s -> {
            log.info("Unknown source: " + s);
        });
        list = purgeProxy.getUnknownSources(POD_ID);
        list.forEach(s -> {
            log.info("Unknown source: " + s);
        });
    }
}
