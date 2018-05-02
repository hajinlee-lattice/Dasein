package com.latticeengines.domain.exposed.datacloud.manage;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class DataCloudVersionUnitTestNG {

    @Test(groups = "unit", dataProvider = "buildNumberProvider")
    public void parseFromBuildNumber(String version, String refresh, String expectedVersion) {
        DataCloudVersion dcVersion = new DataCloudVersion();
        dcVersion.setVersion(version);
        dcVersion.setRefreshVersion(refresh);
        String buildNumber = dcVersion.getDataCloudBuildNumber();
        DataCloudVersion parsed = DataCloudVersion.parseBuildNumber(buildNumber);
        Assert.assertEquals(parsed.getVersion(), expectedVersion);
    }

    @DataProvider(name = "buildNumberProvider")
    public Object[][] provideBuildNumbers() {
        return new Object[][] { //
                { "1.2.3", "12345", "1.2.3" }, //
                { "1.2.3", "", "1.2.3" },
                { "1.2.3", null, "1.2.3" }
        };
    }

}
