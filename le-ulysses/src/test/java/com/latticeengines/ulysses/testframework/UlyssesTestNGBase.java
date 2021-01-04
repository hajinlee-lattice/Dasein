package com.latticeengines.ulysses.testframework;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Listeners;

import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.latticeengines.aws.dynamo.DynamoService;
import com.latticeengines.datafabric.service.datastore.impl.DynamoDataStoreImpl;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;

@Listeners({ GlobalAuthCleanupTestListener.class })
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-ulysses-context.xml" })
public class UlyssesTestNGBase extends AbstractTestNGSpringContextTests {

    @Inject
    protected DynamoService dynamoService;

    @Value("${common.le.environment}")
    private String leEnv;

    @Value("${common.le.stack}")
    private String leStack;

    protected void createTable(String repository, String recordType) {
        String tableName = DynamoDataStoreImpl.buildTableName(repository, recordType);
        dynamoService.deleteTable(tableName);
        dynamoService.createTable(tableName, 10, 10, "Id", ScalarAttributeType.S.name(), null, null, null);
    }

    protected void createCompositeTable(String repository, String recordType) {
        String tableName = DynamoDataStoreImpl.buildTableName(repository, recordType);
        dynamoService.deleteTable(tableName);
        dynamoService.createTable(tableName, 10, 10, "parentKey", ScalarAttributeType.S.name(), "entityId",
                ScalarAttributeType.S.name(), null);
    }
}
