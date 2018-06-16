package com.latticeengines.redis.testframework;


import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-redis-context.xml" })
public class RedisFunctionalTestNGBase extends AbstractTestNGSpringContextTests {



}
