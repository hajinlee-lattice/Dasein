package com.latticeengines.dataplatform.exposed.domain;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class LoadConfigurationUnitTestNG {
    
    @Test(groups = "unit")
    public void testSerDe() {
        LoadConfiguration config = new LoadConfiguration();
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host("localhost").port(3306).db("dataplatformtest").user("root").password("welcome");
        DbCreds creds = new DbCreds(builder);
        config.setCreds(creds);
        config.setCustomer("DELL");
        config.setTable("DELL_EVENT_TABLE_TEST");
        String serializedStr = config.toString();
        System.out.println(serializedStr);
        LoadConfiguration deserializedConfig = JsonUtils.deserialize(serializedStr, LoadConfiguration.class);
        assertEquals(deserializedConfig.getTable(), config.getTable());
        assertEquals(deserializedConfig.getCustomer(), config.getCustomer());
        assertEquals(deserializedConfig.getCreds().getDb(), config.getCreds().getDb());
        assertEquals(deserializedConfig.getCreds().getPort(), config.getCreds().getPort());
        assertEquals(deserializedConfig.getCreds().getHost(), config.getCreds().getHost());
        assertEquals(deserializedConfig.getCreds().getUser(), config.getCreds().getUser());
        assertEquals(deserializedConfig.getCreds().getPassword(), config.getCreds().getPassword());
    }
}
