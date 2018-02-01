package com.latticeengines.cache.service.impl;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import com.latticeengines.cache.exposed.service.CacheServiceBase;
import com.latticeengines.domain.exposed.cache.CacheName;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-cache-context.xml" })
public class CacheServiceTestNG extends AbstractTestNGSpringContextTests {

    @Test(groups = "manual")
    private void clearCache() {
        CacheServiceBase.getCacheService().refreshKeysByPattern("LETest1517445531180", CacheName.values());
    }

}
