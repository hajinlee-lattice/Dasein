package com.latticeengines.redis.service.impl;

import javax.inject.Inject;

import org.testng.annotations.Test;

import com.latticeengines.redis.exposed.service.RedisService;
import com.latticeengines.redis.testframework.RedisFunctionalTestNGBase;

public class RedisServiceImplTestNG extends RedisFunctionalTestNGBase {

    @Inject
    private RedisService redisService;

    @Test(groups = "functional")
    public void testPubSub() {

    }

}
