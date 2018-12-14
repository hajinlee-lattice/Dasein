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
        businessCalendar = createBusinessCalendar(BusinessCalendar.Mode.STARTING_DATE);
    }

    @Test(groups = "deployment", priority = 0)
    public void testSaveBusinessCalendar() {
        Assert.assertNull(periodProxy.getBusinessCalendar(mainCustomerSpace));
        BusinessCalendar calendar = periodProxy.saveBusinessCalendar(mainCustomerSpace, businessCalendar);
        Assert.assertNotNull(calendar);
        Assert.assertEquals(calendar.getStartingDate(), "JAN-15");
    }

    @Test(groups = "deployment", priority = 1)
    public void testGetBusinessCalendar() {
        BusinessCalendar calendar = periodProxy.getBusinessCalendar(mainCustomerSpace);
        Assert.assertNotNull(calendar);
        Assert.assertEquals(calendar.getLongerMonth(), Integer.valueOf(1));
        Assert.assertEquals(calendar.getMode(), BusinessCalendar.Mode.STARTING_DATE);
        Assert.assertEquals(calendar.getStartingDate(), "JAN-15");
    }

    @Test(groups = "deployment", priority = 2)
    public void testDeleteBusinessCalendar() {
        try {
            periodProxy.deleteBusinessCalendar(mainCustomerSpace);
        } catch (Exception exc) {
            Assert.fail("Should not fail.");
        }
    }

    @Test(groups = "deployment", priority = 3)
    public void testStandardTypeForBusinessCalendar() {
        Assert.assertNull(periodProxy.getBusinessCalendar(mainCustomerSpace));

        businessCalendar = createBusinessCalendar(BusinessCalendar.Mode.STANDARD);
        BusinessCalendar calendar = periodProxy.saveBusinessCalendar(mainCustomerSpace, businessCalendar);
        Assert.assertNotNull(calendar);
        Assert.assertNull(calendar.getStartingDate());

        calendar = periodProxy.getBusinessCalendar(mainCustomerSpace);
        Assert.assertNotNull(calendar);
        Assert.assertEquals(calendar.getMode(), BusinessCalendar.Mode.STANDARD);
        Assert.assertNull(calendar.getLongerMonth());
        Assert.assertNull(calendar.getStartingDate());
        Assert.assertNull(calendar.getStartingDay());

        periodProxy.deleteBusinessCalendar(mainCustomerSpace);
        Assert.assertNull(periodProxy.getBusinessCalendar(mainCustomerSpace));
    }

    private BusinessCalendar createBusinessCalendar(BusinessCalendar.Mode mode) {
        BusinessCalendar calendar = new BusinessCalendar();
        calendar.setMode(mode);
        calendar.setStartingDate("JAN-15");
        calendar.setLongerMonth(1);
        return calendar;
    }
}
