package com.latticeengines.datafabric.connector;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ConnectorConfigurationUnitTestNG {

    @Test(groups = "unit")
    public void testConfiguration() {
        Map<String, String> props = new HashMap<>();
        props.put("prop1", "value1");
        props.put("prop2", "value2");

        Connector1 connector1 = new Connector1(props);
        Connector2 connector2 = new Connector2(props);

        String value1 = connector1.getProperty(Connector1.PROP1, String.class);
        Assert.assertEquals(value1, "value1");

        boolean configException = false;
        try {
            value1 = connector1.getProperty(Connector2.PROP2, String.class);
            Assert.assertEquals(value1, "value2");
        } catch (ConfigException e) {
            configException = true;
        }
        Assert.assertTrue(configException, "Should encounter ConfigException.");

        String value2 = connector2.getProperty(Connector2.PROP2, String.class);
        Assert.assertEquals(value2, "value2");

        configException = false;
        try {
            value1 = connector2.getProperty(Connector1.PROP1, String.class);
            Assert.assertEquals(value1, "value1");
        } catch (ConfigException e) {
            configException = true;
        }
        Assert.assertTrue(configException, "Should encounter ConfigException.");
    }

    private static class Connector1 extends ConnectorConfiguration {
        private static ConfigDef config;
       
        @SuppressWarnings("unused")
        public static ConfigDef getConfig() {
            return config;
        }

        static final WorkerProperty<String> PROP1 = new WorkerProperty<>("prop1", "Property 1", "Property 1");
        private static final String GROUP = "group";

        static {
            initialize();
            addGroup(GROUP);
            addPropertyToGroup(PROP1, String.class, GROUP);
            config = tmpConfig.get();
        }

        Connector1(Map<String, String> props) {
            super(config, props);
        }
    }

    private static class Connector2 extends ConnectorConfiguration {
        private static ConfigDef config;

        @SuppressWarnings("unused")
        public static ConfigDef getConfig() {
            return config;
        }

        static final WorkerProperty<String> PROP2 = new WorkerProperty<>("prop2", "Property 2", "Property 2");
        private static final String GROUP = "group";

        static {
            initialize();
            addGroup(GROUP);
            addPropertyToGroup(PROP2, String.class, GROUP);
            config = tmpConfig.get();
        }

        Connector2(Map<String, String> props) {
            super(config, props);
        }
    }

}
