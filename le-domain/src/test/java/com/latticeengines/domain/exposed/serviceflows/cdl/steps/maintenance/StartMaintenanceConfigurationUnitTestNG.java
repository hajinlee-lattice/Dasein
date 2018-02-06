package com.latticeengines.domain.exposed.serviceflows.cdl.steps.maintenance;

import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class StartMaintenanceConfigurationUnitTestNG {

    @Test(groups = "unit")
    public void testNullEntity() {
        StartMaintenanceConfiguration configuration = new StartMaintenanceConfiguration();
        configuration.setCustomerSpace(CustomerSpace.parse("TestTenant"));
        configuration.setPodId("Default");
        configuration.setMicroServiceHostPort("localhost:8080");
        configuration.setInternalResourceHostPort("localhost:8080");
        List<String> entities = configuration.getEntityList();
        Assert.assertTrue(entities.size() == 4);
        Assert.assertNull(configuration.getEntity());
        List<String> expected = new ArrayList<>();
        expected.add(BusinessEntity.Account.name());
        expected.add(BusinessEntity.Contact.name());
        expected.add(BusinessEntity.Product.name());
        expected.add(BusinessEntity.Transaction.name());
        String expectStr = String.join(",", expected);
        Assert.assertEquals(String.join(",", entities), expectStr);
    }
}
