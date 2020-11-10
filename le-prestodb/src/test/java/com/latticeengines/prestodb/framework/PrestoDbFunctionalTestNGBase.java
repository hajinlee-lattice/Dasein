package com.latticeengines.prestodb.framework;


import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-prestodb-context.xml" })
public class PrestoDbFunctionalTestNGBase extends AbstractTestNGSpringContextTests {



}
