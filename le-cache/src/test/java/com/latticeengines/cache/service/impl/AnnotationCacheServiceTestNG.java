package com.latticeengines.cache.service.impl;

import static org.testng.Assert.assertEquals;

import javax.inject.Inject;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.cache.test.annotation.CacheEntity;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-cache-annotation-context.xml" })
public class AnnotationCacheServiceTestNG extends AbstractTestNGSpringContextTests {

    @Inject
    private CacheEntity cacheEntity;

    @BeforeClass
    public void before() {
        cacheEntity.clear(0);
        cacheEntity.clear(1);
    }

    @Test
    public void test() {
        final Integer key = 0;
        cacheEntity.putValueBypassCache(key, 0);

        assertEquals(cacheEntity.getValue(key), Integer.valueOf(0));
        cacheEntity.putValueBypassCache(key, 1);
        assertEquals(cacheEntity.getValue(key), Integer.valueOf(0));
        assertEquals(cacheEntity.getValueBypassCache(key), Integer.valueOf(1));

        cacheEntity.putValue(key, 2);
        assertEquals(cacheEntity.getValue(key), Integer.valueOf(2));
        assertEquals(cacheEntity.getValueBypassCache(key), Integer.valueOf(2));

        cacheEntity.putValue(key, 3);
        assertEquals(cacheEntity.getValue(key), Integer.valueOf(3));
        assertEquals(cacheEntity.getValueBypassCache(key), Integer.valueOf(3));
    }
}
