package com.latticeengines.apps.cdl.entitymgr.impl;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.BusinessCalendarEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.cdl.BusinessCalendar;

public class BusinessCalendarEntityMgrTestNG extends CDLFunctionalTestNGBase  {

    @Inject
    private BusinessCalendarEntityMgr entityMgr;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
    }

    @Test(groups = "functional")
    public void testMultiTenantFilter() {
        Assert.assertNull(entityMgr.find());
        Assert.assertNull(entityMgr.delete());

        BusinessCalendar calendar = constructCalendar();
        BusinessCalendar saved = entityMgr.save(calendar);
        Assert.assertNotNull(saved);
        Assert.assertNotNull(saved.getPid());

        Assert.assertNotNull(entityMgr.find());

        Tenant tenant = new Tenant("dummy");
        tenant.setPid(-1L);
        MultiTenantContext.setTenant(tenant);
        Assert.assertNull(entityMgr.find());

        MultiTenantContext.setTenant(mainTestTenant);
        Assert.assertNotNull(entityMgr.delete());
    }

    private BusinessCalendar constructCalendar() {
        BusinessCalendar calendar = new BusinessCalendar();
        calendar.setMode(BusinessCalendar.Mode.STARTING_DATE);
        calendar.setStartingDate("JAN-15");
        calendar.setLongerMonth(1);
        return calendar;
    }

}
