package com.latticeengines.apps.cdl.controller;

import java.util.Collections;

import javax.inject.Inject;

import org.springframework.retry.support.RetryTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.DataDeleteOperationConfiguration;
import com.latticeengines.domain.exposed.metadata.DataOperation;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;

public class DataOperationResourceDeploymentTestNG extends CDLDeploymentTestNGBase {

    @Inject
    private CDLProxy cdlProxy;

    private RetryTemplate retry;

    @BeforeClass(groups = "deployment-app")
    public void setup() throws Exception {
        setupTestEnvironment();
        MultiTenantContext.setTenant(mainTestTenant);
        retry = RetryUtils.getRetryTemplate(10, //
                Collections.singleton(AssertionError.class), null);
    }

    @Test(groups = "deployment-app")
    public void testCrud() {
        String tenantId = mainTestTenant.getId();
        DataDeleteOperationConfiguration configuration = new DataDeleteOperationConfiguration();
        configuration.setIdEntity(BusinessEntity.Account);
        configuration.setDeleteType(DataDeleteOperationConfiguration.DeleteType.SOFT);
        String dropPath = cdlProxy.createDataOperation(tenantId, DataOperation.OperationType.DELETE, configuration);
        Assert.assertNotNull(dropPath);

        retry.execute(context -> {
            DataOperation dataOperation = cdlProxy.findDataOperationByDropPath(tenantId, dropPath);
            Assert.assertNotNull(dataOperation);
            Assert.assertEquals(dataOperation.getDropPath(), dropPath);
            Assert.assertEquals(((DataDeleteOperationConfiguration)dataOperation.getConfiguration()).getDeleteType(),
                    DataDeleteOperationConfiguration.DeleteType.SOFT);
            return true;
        });
    }
}
