package com.latticeengines.domain.exposed.eai.route;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class CamelRouteConfigurationUnitTestNG {

    @Test(groups = "unit")
    public void testSerDe() {
        SftpToHdfsRouteConfiguration sftpToHdfsRouteConfiguration = new SftpToHdfsRouteConfiguration();
        String json = JsonUtils.serialize(sftpToHdfsRouteConfiguration);

        CamelRouteConfiguration camelRouteConfiguration = JsonUtils.deserialize(json, CamelRouteConfiguration.class);
        Assert.assertTrue(camelRouteConfiguration instanceof  SftpToHdfsRouteConfiguration);
    }

}
