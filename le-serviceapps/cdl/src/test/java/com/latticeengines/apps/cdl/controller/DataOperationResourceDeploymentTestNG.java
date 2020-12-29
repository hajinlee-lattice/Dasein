package com.latticeengines.apps.cdl.controller;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.DataDeleteOperationConfiguration;
import com.latticeengines.domain.exposed.metadata.DataOperation;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;

public class DataOperationResourceDeploymentTestNG extends CDLDeploymentTestNGBase {

    @Inject
    private CDLProxy cdlProxy;

    @BeforeClass(groups = "deployment-app")
    public void setup() throws Exception {
        setupTestEnvironment();
        MultiTenantContext.setTenant(mainTestTenant);
    }

    @Test(groups = "deployment-app")
    public void testCrudListSegment() {
        String tenantId = mainTestTenant.getId();
        DataDeleteOperationConfiguration configuration = new DataDeleteOperationConfiguration();
        configuration.setEntity(BusinessEntity.Account);
        configuration.setSystemName("DefaultSystem");
        configuration.setDeleteType(DataDeleteOperationConfiguration.DeleteType.SOFT);
        String dropPath = cdlProxy.createDataOperation(tenantId, DataOperation.OperationType.DELETE, configuration);
        Assert.assertNotNull(dropPath);

        DataOperation dataOperation = cdlProxy.findDataOperationByDropPath(tenantId, dropPath);
        Assert.assertNotNull(dataOperation);
        Assert.assertNotNull(dataOperation);
        Assert.assertEquals(dataOperation.getDropPath(), dropPath);
        Assert.assertEquals(((DataDeleteOperationConfiguration)dataOperation.getConfiguration()).getDeleteType(),
                DataDeleteOperationConfiguration.DeleteType.SOFT);
    }
}
