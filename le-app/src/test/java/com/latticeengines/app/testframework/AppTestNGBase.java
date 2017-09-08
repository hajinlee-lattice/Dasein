package com.latticeengines.app.testframework;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Listeners;

import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.latticeengines.aws.dynamo.DynamoService;
import com.latticeengines.common.exposed.util.SSLUtils;
import com.latticeengines.datafabric.service.datastore.impl.DynamoDataStoreImpl;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;
import com.latticeengines.testframework.service.impl.GlobalAuthFunctionalTestBed;

@Listeners({ GlobalAuthCleanupTestListener.class })
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-app-context.xml" })
public class AppTestNGBase extends AbstractTestNGSpringContextTests {

    @Autowired
    protected GlobalAuthFunctionalTestBed globalAuthFunctionalTestBed;

    @Autowired
    protected DynamoService dynamoService;

    @Value("${common.le.environment}")
    private String leEnv;

    @Value("${common.le.stack}")
    private String leStack;

    protected Tenant mainTestTenant;

    protected void setupTestEnvironmentWithOneTenant()
            throws NoSuchAlgorithmException, KeyManagementException, IOException {
        SSLUtils.turnOffSSL();
        globalAuthFunctionalTestBed.bootstrap(1);
        mainTestTenant = globalAuthFunctionalTestBed.getMainTestTenant();
    }

    protected void createTable(String repository, String recordType) {
        String tableName = DynamoDataStoreImpl.buildTableName(repository, recordType);
        dynamoService.deleteTable(tableName);
        dynamoService.createTable(tableName, 10, 10, "Id", ScalarAttributeType.S.name(), null, null);
    }

    protected void createCompositeTable(String repository, String recordType) {
        String tableName = DynamoDataStoreImpl.buildTableName(repository, recordType);
        dynamoService.deleteTable(tableName);
        dynamoService.createTable(tableName, 10, 10, "parentKey", ScalarAttributeType.S.name(), "entityId",
                ScalarAttributeType.S.name());
    }
}