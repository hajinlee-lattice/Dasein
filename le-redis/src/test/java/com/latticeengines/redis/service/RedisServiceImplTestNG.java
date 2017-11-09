package com.latticeengines.redis.service;

import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cache.CacheNames;
import com.latticeengines.redis.exposed.service.RedisService;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-redis-context.xml" })
public class RedisServiceImplTestNG extends AbstractTestNGSpringContextTests {

    @Autowired
    private RedisService redisService;

    @Test(groups = { "functional" })
    private void test() {
        // redisson.getMap("DataLakeCache").put("ASF|C", "BC");
        Set<Object> set = redisService.getKeys(CacheNames.DataLakeCMCache.toString(),
                "^TFTest_4.TFTest_4.Production.*");

        System.out.println(set);
    }
}
