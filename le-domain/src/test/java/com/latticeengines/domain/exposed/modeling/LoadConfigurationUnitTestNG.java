package com.latticeengines.domain.exposed.modeling;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.JsonUtils;

public class LoadConfigurationUnitTestNG {

    private static final String PASSWORD = "welcome";

    @SuppressWarnings("deprecation")
    @Test(groups = "unit")
    public void testSerDe() {
        LoadConfiguration config = new LoadConfiguration();
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host("localhost").port(3306).db("dataplatformtest").user("root").password(PASSWORD).dbType("MySQL");
        DbCreds creds = new DbCreds(builder);
        config.setCreds(creds);
        config.setCustomer("DELL");
        config.setTable("DELL_EVENT_TABLE_TEST");
        config.setMetadataTable("METADATA_TABLE");
        String serializedStr = config.toString();
        System.out.println(serializedStr);
        LoadConfiguration deserializedConfig = JsonUtils.deserialize(serializedStr, LoadConfiguration.class);
        assertEquals(deserializedConfig.getTable(), config.getTable());
        assertEquals(deserializedConfig.getMetadataTable(), config.getMetadataTable());
        assertEquals(deserializedConfig.getCustomer(), config.getCustomer());
        assertEquals(deserializedConfig.getCreds().getDb(), config.getCreds().getDb());
        assertEquals(deserializedConfig.getCreds().getPort(), config.getCreds().getPort());
        assertEquals(deserializedConfig.getCreds().getHost(), config.getCreds().getHost());
        assertEquals(deserializedConfig.getCreds().getUser(), config.getCreds().getUser());
        assertEquals(deserializedConfig.getCreds().getDecryptedPassword(), PASSWORD);
        assertEquals(deserializedConfig.getCreds().getDbType(), config.getCreds().getDbType());
    }

    @Test(groups = "unit")
    public void testSerDeEncrypted() {
        LoadConfiguration config = new LoadConfiguration();
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host("localhost").port(3306).db("dataplatformtest").user("root")
                .encryptedPassword(CipherUtils.encrypt(PASSWORD)).dbType("MySQL");
        DbCreds creds = new DbCreds(builder);
        config.setCreds(creds);
        config.setCustomer("DELL");
        config.setTable("DELL_EVENT_TABLE_TEST");
        config.setMetadataTable("METADATA_TABLE");
        String serializedStr = config.toString();
        System.out.println(serializedStr);
        LoadConfiguration deserializedConfig = JsonUtils.deserialize(serializedStr, LoadConfiguration.class);
        assertEquals(deserializedConfig.getTable(), config.getTable());
        assertEquals(deserializedConfig.getMetadataTable(), config.getMetadataTable());
        assertEquals(deserializedConfig.getCustomer(), config.getCustomer());
        assertEquals(deserializedConfig.getCreds().getDb(), config.getCreds().getDb());
        assertEquals(deserializedConfig.getCreds().getPort(), config.getCreds().getPort());
        assertEquals(deserializedConfig.getCreds().getHost(), config.getCreds().getHost());
        assertEquals(deserializedConfig.getCreds().getUser(), config.getCreds().getUser());
        assertEquals(deserializedConfig.getCreds().getDecryptedPassword(), PASSWORD);
        assertEquals(deserializedConfig.getCreds().getDbType(), config.getCreds().getDbType());
    }
}
