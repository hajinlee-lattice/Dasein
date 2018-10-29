package com.latticeengines.apps.cdl.controller;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.domain.exposed.serviceapps.cdl.BusinessCalendar;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;

public class PeriodResourceDeploymentTestNG extends CDLDeploymentTestNGBase {

    @Inject
    private PeriodProxy periodProxy;

    private BusinessCalendar businessCalendar;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironment();
        businessCalendar = createBusinessCalendar();
    }

    @Test(groups = "deployment")
    public void testSaveBusinessCalendar() {
        Assert.assertNull(periodProxy.getBusinessCalendar(mainCustomerSpace));

        BusinessCalendar calendar = periodProxy.saveBusinessCalendar(mainCustomerSpace, businessCalendar);
        Assert.assertNotNull(calendar);
        Assert.assertNotNull(calendar.getPid());
        Assert.assertEquals(calendar.getStartingDate(), "JAN-15");
    }

    @Test(groups = "deployment", dependsOnMethods = "testSaveBusinessCalendar")
    public void testGetBusinessCalendar() {
        BusinessCalendar calendar = periodProxy.getBusinessCalendar(mainCustomerSpace);
        Assert.assertNotNull(calendar);
        Assert.assertNotNull(calendar.getPid());
        Assert.assertEquals(calendar.getLongerMonth(), Integer.valueOf(1));
        Assert.assertEquals(calendar.getMode(), BusinessCalendar.Mode.STARTING_DATE);
        Assert.assertEquals(calendar.getStartingDate(), "JAN-15");
    }

    @Test(groups = "deployment", dependsOnMethods = { "testSaveBusinessCalendar", "testGetBusinessCalendar" })
    public void testDeleteBusinessCalendar() {
        try {
            periodProxy.deleteBusinessCalendar(mainCustomerSpace);
        } catch (Exception exc) {
            Assert.fail("Should not fail.");
        }
    }

    private BusinessCalendar createBusinessCalendar() {
        BusinessCalendar calendar = new BusinessCalendar();
        calendar.setMode(BusinessCalendar.Mode.STARTING_DATE);
        calendar.setStartingDate("JAN-15");
        calendar.setLongerMonth(1);
        return calendar;
    }
}
