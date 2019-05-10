package com.latticeengines.aws.dynamo.impl;

import javax.inject.Inject;

import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.latticeengines.aws.dynamo.DynamoService;

@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-aws-context.xml" })
public class DynamoServiceImplTestNG extends AbstractTestNGSpringContextTests {

    @Inject
    private DynamoService dynamoService;

    @Test(groups = "functional")
    public void testDescribeTable() {
        TableDescription description = dynamoService.describeTable("EntitySeedLookup_Staging_20190305");
        Assert.assertNotNull(description);
        Assert.assertFalse(description.getKeySchema().isEmpty());
        description = dynamoService.describeTable("EntitySeedLookup_Serving_20190305");
        Assert.assertNotNull(description);
        Assert.assertFalse(description.getKeySchema().isEmpty());
    }

}
