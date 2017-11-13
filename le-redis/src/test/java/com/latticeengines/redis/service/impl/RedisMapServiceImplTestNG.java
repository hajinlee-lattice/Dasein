package com.latticeengines.redis.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.redis.exposed.service.RedisMapService;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-redis-context.xml" })
public class RedisMapServiceImplTestNG extends AbstractTestNGSpringContextTests {

    @Inject
    private RedisMapService redisMapService;

    private static final String TEST_MAP_NAME = "TEST_MAP";

    @BeforeClass(groups = { "functional" })
    public void setup() {
        redisMapService.deleteAllKeys(TEST_MAP_NAME);
    }

    @AfterClass(groups = { "functional" })
    public void cleanup() {
        redisMapService.deleteAllKeys(TEST_MAP_NAME);
    }

    @Test(groups = { "functional" })
    private void test() {
        assertTrue(redisMapService.fastPutValue(TEST_MAP_NAME, "ABC", "abc"));
        assertFalse(redisMapService.fastPutValue(TEST_MAP_NAME, "ABC", "abc"));
        assertEquals(redisMapService.getValue(TEST_MAP_NAME, "ABC"), "abc");

        assertFalse(redisMapService.fastPutValue(TEST_MAP_NAME, "ABC", "abc1"));
        assertEquals(redisMapService.getValue(TEST_MAP_NAME, "ABC"), "abc1");

        assertTrue(redisMapService.fastPutValue(TEST_MAP_NAME, "EFG", "efg"));
        assertEquals(
                redisMapService.getValues(TEST_MAP_NAME, //
                        Arrays.stream(new String[] { "ABC", "EFG" }).collect(Collectors.toSet())).size(), //
                2);

        assertEquals(redisMapService.deleteKeysByPattern(TEST_MAP_NAME, ".*B.*").longValue(), 1L);
        assertEquals(redisMapService.getValue(TEST_MAP_NAME, "EFG"), "efg");

        assertTrue(redisMapService.deleteAllKeys(TEST_MAP_NAME));
        assertTrue(redisMapService.getKeys(TEST_MAP_NAME).isEmpty());
    }
}
