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
import com.latticeengines.domain.exposed.cdl.DataDeleteOperationConfiguration;
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
        DataDeleteOperationConfiguration configuration = new DataDeleteOperationConfiguration();
        configuration.setIdEntity(BusinessEntity.Account);
        configuration.setSystemName("DefaultSystem");
        configuration.setDeleteType(DataDeleteOperationConfiguration.DeleteType.SOFT);
        String dropPath = dataOperationService.createDataOperation(mainCustomerSpace, DataOperation.OperationType.DELETE,configuration);
        Assert.assertNotNull(dropPath);
        System.out.println(dropPath);

        retry.execute(context -> {
            DataOperation dataOperation = dataOperationService.findDataOperationByDropPath(mainCustomerSpace, dropPath);
            Assert.assertNotNull(dataOperation);
            Assert.assertEquals(dataOperation.getDropPath(), dropPath);
            Assert.assertEquals(((DataDeleteOperationConfiguration)dataOperation.getConfiguration()).getDeleteType(),
                    DataDeleteOperationConfiguration.DeleteType.SOFT);
            return true;
        });
        List<DataOperation> dataOperations = dataOperationService.findAllDataOperation(mainCustomerSpace);
        Assert.assertNotNull(dataOperations);
        Assert.assertEquals(dataOperations.size(), 1);
        Assert.assertEquals(dataOperations.get(0).getDropPath(), dropPath);

        dataOperationService.createDataOperation(mainCustomerSpace, DataOperation.OperationType.DELETE,configuration);
        dataOperations = dataOperationService.findAllDataOperation(mainCustomerSpace);
        Assert.assertNotNull(dataOperations);
        Assert.assertEquals(dataOperations.size(), 1);
        Assert.assertEquals(dataOperations.get(0).getDropPath(), dropPath);

        dataOperationService.deleteDataOperation(mainCustomerSpace, dataOperations.get(0));
    }
}
