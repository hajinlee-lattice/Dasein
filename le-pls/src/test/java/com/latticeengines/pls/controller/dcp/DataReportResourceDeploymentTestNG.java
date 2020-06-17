package com.latticeengines.pls.controller.dcp;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.dcp.DataReport;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;
import com.latticeengines.pls.functionalframework.DCPDeploymentTestNGBase;
import com.latticeengines.testframework.exposed.proxy.pls.TestDataReportProxy;

public class DataReportResourceDeploymentTestNG extends DCPDeploymentTestNGBase {

    @Inject
    private TestDataReportProxy testDataReportProxy;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.DCP);
        MultiTenantContext.setTenant(mainTestTenant);
        attachProtectedProxy(testDataReportProxy);
    }

    @Test(groups = "deployment")
    public void testGetDataReport() {
        DataReport dataReport = testDataReportProxy.getDataReport(DataReportRecord.Level.Tenant, null, Boolean.TRUE);
        Assert.assertNotNull(dataReport);
        dataReport = testDataReportProxy.getDataReport(DataReportRecord.Level.Source, "sourceId", Boolean.TRUE);
        Assert.assertNotNull(dataReport);
        System.out.println(JsonUtils.serialize(dataReport));
        Assert.assertThrows(() -> testDataReportProxy.getDataReport(DataReportRecord.Level.Upload, null, Boolean.TRUE));
    }
}
