package com.latticeengines.apps.cdl.service.impl;

import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.DataOperationService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.cdl.DataOperationConfiguration;
import com.latticeengines.domain.exposed.metadata.DataOperation;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class DataOperationServiceImplTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DataOperationServiceImplTestNG.class);

    @Inject
    private DataOperationService dataOperationService;

    private RetryTemplate retry;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
        retry = RetryUtils.getRetryTemplate(10, //
                Collections.singleton(AssertionError.class), null);
    }

    @Test(groups = "functional")
    public void testCRUD() {
        DataOperationConfiguration configuration = new DataOperationConfiguration();
        configuration.setEntity(BusinessEntity.Account);
        configuration.setSystemName("DefaultSystem");
        String dropPath = dataOperationService.createDataOperation(mainCustomerSpace, DataOperation.OperationType.DELETE,configuration );
        Assert.assertNotNull(dropPath);
        System.out.println(dropPath);

        List<DataOperation> dataOperations = dataOperationService.findAllDataOperation(mainCustomerSpace);

        Assert.assertNotNull(dataOperations);
        Assert.assertEquals(dataOperations.size(), 1);
        Assert.assertEquals(dataOperations.get(0).getDropPath(), dropPath);
        dataOperationService.deleteDataOperation(mainCustomerSpace, dataOperations.get(0));
    }
}
