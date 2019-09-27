package com.latticeengines.yarn.exposed.client;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
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

    @Test(groups = "unit", dataProvider = "xssSettings")
    public void getXssSetting(String stackSize, String expectedValue) {
        customization = new DefaultYarnClientCustomization(yarnConfiguration, null, null, null,
                null, null);
        if (stackSize != null) {
            containerProperties.setProperty(AppMasterProperty.JVM_STACK.name(), stackSize);
        }
        String xmx = customization.getXssSetting(containerProperties);
        assertEquals(xmx, expectedValue);
    }

    @DataProvider(name = "xssSettings")
    public Object[][] getXssSettings() {
        return new Object[][]{ //
                {"1024", "-Xss1024k"},
                {null, "-Xss1024k"},
                {"2048", "-Xss2048k"},
                {"", "-Xss1024k"},
        };
    }

    @AfterMethod(groups = "unit")
    public void tearDownMethod() {
        containerProperties.clear();
    }

    @Test(groups = "unit", dataProvider = "settings")
    public void getXmxSetting(int minAllocationInMb, String requestedMemory, boolean byFitting, String expectedValue) {
        when(yarnConfiguration.getInt(anyString(), anyInt())).thenReturn(minAllocationInMb);
        customization = new DefaultYarnClientCustomization(yarnConfiguration, null, null, null,
                null, null);
        if (requestedMemory != null) {
            containerProperties.setProperty(ContainerProperty.MEMORY.name(), requestedMemory);
        }

        String xmx = customization.getXmxSetting(containerProperties, byFitting);
        assertEquals(xmx, expectedValue);
    }

    // minAllocationMb, requestedMemory, byFitting, expectedXmx
    @DataProvider(name = "settings")
    public Object[][] getSettings() {
        String expected = "-Xms" + (2048 - 512) + "m -Xmx" + +(2048 - 512) + "m";
        return new Object[][] { //
                // byFitting = false is old behavior to test
                // calHeapSizeByFixedPara
                { 1024, "2048", false, expected }, //
                { -1, "2048", false, expected }, //
                { 2048, "1024", false, expected }, //
                { 2048, null, false, expected }, //
                { -1, null, false, "-Xms1024m -Xmx1024m" }, //

                // byFitting = true is new havior to test calHeapSizeByFitting
                // different combination of minAllocationMb and requestedMemory
                // is covered in test case of calHeapSizeByFixedPara
                // mem in (, 1536) -> heap = 1024
                { 1024, "1024", true, "-Xms1024m -Xmx1024m" }, //
                { 1280, "1280", true, "-Xms1024m -Xmx1024m" }, //
                // mem in [1536, 2048] -> heap = mem - 512
                { 1536, "1536", true, "-Xms1024m -Xmx1024m" }, //
                { 1792, "1792", true, "-Xms1280m -Xmx1280m" }, //
                { 2048, "2048", true, "-Xms1536m -Xmx1536m" }, //
                // mem in (2048, 4096] -> heap = mem - 1024
                { 4096, "4096", true, "-Xms3072m -Xmx3072m" }, //
                // mem in (4096, 8192] -> heap = mem - 1536
                { 6144, "6144", true, "-Xms4608m -Xmx4608m" }, //
                { 8192, "8192", true, "-Xms6656m -Xmx6656m" }, //
        };
    }
}
