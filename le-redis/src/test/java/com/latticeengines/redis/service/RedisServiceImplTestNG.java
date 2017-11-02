package com.latticeengines.redis.service;

import java.util.Set;

import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-redis-context.xml" })
public class RedisServiceImplTestNG extends AbstractTestNGSpringContextTests {

    @Autowired
    private RedissonClient redisson;

    @Test(groups = { "functional" })
    private void test() {
        Set<Object> set = redisson.getMap("DataLakeCache").keySet("*FisherScientific.FisherScientific.Production*");
        // for (Object o : set) {
        // System.out.println(o);
        // }
        // redisson.getMap("DataLakeCache").fastRemove(set.toArray());
        // set.clear();
        System.out.println(set.size());
    }
}
