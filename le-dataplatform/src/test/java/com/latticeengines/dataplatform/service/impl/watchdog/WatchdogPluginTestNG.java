package com.latticeengines.dataplatform.service.impl.watchdog;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Map;

import org.testng.annotations.Test;

import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;

public class WatchdogPluginTestNG extends DataPlatformFunctionalTestNGBase {

    @Test(groups = { "functional.platform" })
    public void register() {
        Map<String, WatchdogPlugin> registry = WatchdogPlugin.getPlugins();
        assertEquals(registry.size(), 1);
        assertTrue(registry.keySet().contains("GenerateYarnMetrics"));
    }
}
