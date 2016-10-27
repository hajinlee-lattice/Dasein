package com.latticeengines.sampleapi.sample.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import com.latticeengines.actors.visitor.sample.SampleActorSystemWrapper;
import com.latticeengines.sampleapi.sample.service.SampleService;

@Test
@ContextConfiguration(locations = { "classpath:test-sample-service-context.xml" })
public class SampleServiceImplUnitTestNG extends AbstractTestNGSpringContextTests {
    @Autowired
    private SampleActorSystemWrapper wrapper;

    @Autowired
    private SampleService service;

    @Test(groups = { "unit" })
    public void test() throws Exception {
        try {
            List<Object> sampleRequests = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                Object msg = UUID.randomUUID().toString();
                sampleRequests.add(msg);
            }

            int idx = 0;
            for (Object result : service.doSampleWork(sampleRequests)) {
                System.out.println(result);
                Assert.assertNotNull(result);
                Assert.assertNotEquals(result, sampleRequests.get(idx++));
            }
        } finally {
            wrapper.shutdown();
        }
    }
}
