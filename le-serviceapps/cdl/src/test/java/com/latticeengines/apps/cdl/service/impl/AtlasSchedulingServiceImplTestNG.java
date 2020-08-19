package com.latticeengines.apps.cdl.service.impl;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.AtlasSchedulingService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.domain.exposed.cdl.AtlasScheduling;

public class AtlasSchedulingServiceImplTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private AtlasSchedulingService atlasSchedulingService;

    private static final String DEFAULT_CRON = "0 0 0 31 DEC ? 2099";

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironmentWithDataCollection();
    }

    @Test(groups = "functional")
    public void testAtlasScheduling() {
        AtlasScheduling scheduling = atlasSchedulingService.findSchedulingByType(mainCustomerSpace,
                AtlasScheduling.ScheduleType.Export);
        Assert.assertNull(scheduling);
        atlasSchedulingService.createOrUpdateSchedulingByType(mainCustomerSpace, "error cron", AtlasScheduling.ScheduleType.Export);
        scheduling = atlasSchedulingService.findSchedulingByType(mainCustomerSpace,
                AtlasScheduling.ScheduleType.Export);
        Assert.assertNotNull(scheduling);
        Assert.assertEquals(scheduling.getCronExpression(), DEFAULT_CRON);
        atlasSchedulingService.createOrUpdateSchedulingByType(mainCustomerSpace, "0/30 * * * * ?", AtlasScheduling.ScheduleType.Export);
        scheduling = atlasSchedulingService.findSchedulingByType(mainCustomerSpace,
                AtlasScheduling.ScheduleType.Export);
        Assert.assertNotNull(scheduling);
        Assert.assertEquals(scheduling.getCronExpression(), "0/30 * * * * ?");
        scheduling = atlasSchedulingService.findSchedulingByType(mainCustomerSpace, AtlasScheduling.ScheduleType.PA);
        Assert.assertNull(scheduling);
        atlasSchedulingService.createOrUpdateSchedulingByType(mainCustomerSpace, "", AtlasScheduling.ScheduleType.PA);
        scheduling = atlasSchedulingService.findSchedulingByType(mainCustomerSpace, AtlasScheduling.ScheduleType.PA);
        Assert.assertNotNull(scheduling);
        Assert.assertEquals(scheduling.getCronExpression(), DEFAULT_CRON);
    }

}
