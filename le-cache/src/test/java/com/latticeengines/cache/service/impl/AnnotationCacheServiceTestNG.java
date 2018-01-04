package com.latticeengines.cache.service.impl;

import static org.testng.Assert.assertEquals;

import org.springframework.beans.factory.annotation.Autowired;
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

    @Autowired
    private CacheEntity cacheEntity;

    @BeforeClass
    public void before() {
        cacheEntity.clear(0);
        cacheEntity.clear(1);
    }

    @Test
    public void test() {
        int k = 0;
        int v = 0;
        assertEquals(cacheEntity.getValue(k), 0);
        cacheEntity.putValue(++v);
        assertEquals(cacheEntity.getValue(k), 0);
        assertEquals(cacheEntity.getValue(v), 1);
    }
}
