package com.latticeengines.swlib.service.impl;

import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

public class SoftwareLibraryUnitTestNG {

    @Test(groups = "unit")
    public void testLoadingSequence() {
        List<SoftwareLibrary> libs = SoftwareLibrary.LeadPrioritization.getLoadingSequence();
        Assert.assertEquals(libs.size(), 2);
        Assert.assertEquals(libs.get(0), SoftwareLibrary.DataCloud);
        Assert.assertEquals(libs.get(1), SoftwareLibrary.LeadPrioritization);

        libs = SoftwareLibrary.DataCloud.getLoadingSequence();
        Assert.assertEquals(libs.size(), 1);
        Assert.assertEquals(libs.get(0), SoftwareLibrary.DataCloud);

        libs = SoftwareLibrary.getLoadingSequence(Arrays.asList(
                SoftwareLibrary.LeadPrioritization,
                SoftwareLibrary.DataCloud,
                SoftwareLibrary.CDL
        ));
        Assert.assertEquals(libs.size(), 3);
        Assert.assertEquals(libs.get(0), SoftwareLibrary.DataCloud);
    }

}
