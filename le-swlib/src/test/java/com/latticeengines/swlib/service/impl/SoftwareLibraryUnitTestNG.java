package com.latticeengines.swlib.service.impl;

import static com.latticeengines.domain.exposed.swlib.SoftwareLibrary.CDL;
import static com.latticeengines.domain.exposed.swlib.SoftwareLibrary.DataCloud;
import static com.latticeengines.domain.exposed.swlib.SoftwareLibrary.LeadPrioritization;
import static com.latticeengines.domain.exposed.swlib.SoftwareLibrary.Module.dataflowapi;
import static com.latticeengines.domain.exposed.swlib.SoftwareLibrary.Module.workflowapi;

import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;

import edu.emory.mathcs.backport.java.util.Collections;

public class SoftwareLibraryUnitTestNG {

    @Test(groups = "unit")
    public void testLoadingSequence() {
        for (SoftwareLibrary lib: SoftwareLibrary.values()) {
            Assert.assertEquals(lib.getLoadingSequence(dataflowapi), Collections.singletonList(lib));
            if (DataCloud.equals(lib)) {
                Assert.assertEquals(lib.getLoadingSequence(workflowapi), Collections.singletonList(lib));
            } else {
                Assert.assertEquals(lib.getLoadingSequence(workflowapi), Arrays.asList(DataCloud, lib));
            }
        }

        List<SoftwareLibrary> libs = SoftwareLibrary.getLoadingSequence(workflowapi,
                Arrays.asList(LeadPrioritization, CDL));
        Assert.assertEquals(libs.size(), 3);
        Assert.assertEquals(libs.get(0), DataCloud);

        libs = SoftwareLibrary.getLoadingSequence(dataflowapi,
                Arrays.asList(LeadPrioritization, CDL));
        Assert.assertEquals(libs.size(), 2);
    }

}
