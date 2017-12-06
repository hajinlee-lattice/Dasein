package com.latticeengines.cdl.workflow.steps;

import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class UpdateStatsObjectsUnitTestNG {

    private CombineStatistics updateStatsObjects = new CombineStatistics();

    private static final String RESOURCE_ROOT = "com/latticeengines/cdl/workflow/steps/updateStatsObjects";

    @BeforeTest(groups = "unit")
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test(groups = "unit")
    public void testConstructStatsContainer() {

    }

}
