package com.latticeengines.dataplatform.exposed.yarn.client;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class DefaultYarnClientCustomizationUnitTestNG {
    
    private DefaultYarnClientCustomization customization = null;
    private Configuration yarnConfiguration = null;
    private Properties containerProperties = null;
    
    @BeforeClass(groups = "unit")
    public void setup() throws Exception {
        yarnConfiguration = mock(Configuration.class);
        containerProperties = new Properties();
    }
    
    @AfterMethod(groups = "unit")
    public void tearDownMethod() {
        containerProperties.clear();
    }

    @Test(groups = "unit", dataProvider = "settings")
    public void getXmxSetting(int minAllocationInMb, String requestedMemory, String expectedValue) {
        when(yarnConfiguration.getInt(anyString(), anyInt())).thenReturn(minAllocationInMb);
        customization = new DefaultYarnClientCustomization(yarnConfiguration, null, null, null);
        if (requestedMemory != null) {
            containerProperties.setProperty(ContainerProperty.MEMORY.name(), requestedMemory);
        }
        
        String xmx = customization.getXmxSetting(containerProperties);
        assertEquals(xmx, expectedValue);
    }
    
    @DataProvider(name = "settings")
    public Object[][] getSettings() {
        return new Object[][] {
                { 1024, "2048", "-Xmx" + (2048 - 512) + "m -XX:PermSize=256m -XX:MaxPermSize=256m"}, //
                { -1, "2048", "-Xmx" + (2048 - 512) + "m -XX:PermSize=256m -XX:MaxPermSize=256m"}, //
                { 2048, "1024", "-Xmx" + (2048 - 512) + "m -XX:PermSize=256m -XX:MaxPermSize=256m"}, //
                { 2048, null, "-Xmx" + (2048 - 512) + "m -XX:PermSize=256m -XX:MaxPermSize=256m"}, //
                { -1, null, "-Xmx1024m -XX:PermSize=256m -XX:MaxPermSize=256m"}, //
        };
    }
}
