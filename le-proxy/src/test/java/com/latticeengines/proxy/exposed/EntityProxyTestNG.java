package com.latticeengines.proxy.exposed;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;

@ContextConfiguration(locations = { "classpath:test-proxy-context.xml" })
public class EntityProxyTestNG extends AbstractTestNGSpringContextTests {

    @Autowired
    private EntityProxy entityProxy;

    @Test(groups = { "manual" })
    public void test() throws IOException {
        FrontEndQuery frontEndQuery = JsonUtils.deserialize(
                FileUtils.openInputStream(
                        new File(ClassLoader.getSystemClassLoader().getResource("query.json").getPath())),
                FrontEndQuery.class);
        entityProxy.getData("CDLTest_Lynn_0920.CDLTest_Lynn_0920.Production", frontEndQuery);
    }
}
