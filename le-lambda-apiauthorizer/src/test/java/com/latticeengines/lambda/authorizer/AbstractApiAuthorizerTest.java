package com.latticeengines.lambda.authorizer;

import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

@ContextConfiguration(classes = ApplicationConfiguration.class)
@ActiveProfiles("qacluster")
public class AbstractApiAuthorizerTest extends AbstractTestNGSpringContextTests{

}
