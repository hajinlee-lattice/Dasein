package com.latticeengines.aws.dynamo.impl;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

import com.latticeengines.aws.dynamo.DynamoItemService;
import com.latticeengines.aws.dynamo.DynamoService;

@ContextConfiguration(locations = { "classpath:test-aws-context.xml" })
public abstract class DynamoFunctionalTestNGBase extends AbstractTestNGSpringContextTests {
    static final String PARTITION_KEY = "PartitionId";
    static final String SORT_KEY = "SortId";

    @Value("${common.le.environment}")
    String env;

    @Value("${common.le.stack}")
    String stack;

    @Inject
    DynamoService dynamoService;

    @Inject
    DynamoItemService dynamoItemService;

    String tableName;
}
