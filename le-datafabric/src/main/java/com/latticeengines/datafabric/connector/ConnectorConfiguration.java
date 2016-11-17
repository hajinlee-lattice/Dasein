package com.latticeengines.datafabric.connector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import io.confluent.connect.avro.AvroData;

public abstract class ConnectorConfiguration extends AbstractConfig {

    protected static ThreadLocal<ConfigDef> tmpConfig = new ThreadLocal<>();
    private static ThreadLocal<Map<String, List<String>>> tmpGroups = new ThreadLocal<>();

    private static final WorkerProperty<Integer> SCHEMA_CACHE_SIZE = new WorkerProperty<Integer>("schema.cache.size",
            "The size of the schema cache used in the Avro converter.", "Schema Cache Size").setDefaultValue(1000) //
                    .setWidth(ConfigDef.Width.SHORT);

    private static final String SCHEMA_GROUP = "Schema";

    protected static void initialize() {
        tmpConfig.set(new ConfigDef());
        tmpGroups.set(new HashMap<String, List<String>>());
        addGroup(SCHEMA_GROUP);
        addPropertyToGroup(SCHEMA_CACHE_SIZE, Integer.class, SCHEMA_GROUP);
    }

    protected static void addGroup(String group) {
        if (tmpGroups.get().containsKey(group)) {
            throw new RuntimeException(String.format("Group %s already exists.", group));
        }
        tmpGroups.get().put(group, new ArrayList<String>());
    }

    protected static <T> void addPropertyToGroup(WorkerProperty<T> property, Class<T> javaType, String group) {
        if (!tmpGroups.get().containsKey(group)) {
            throw new RuntimeException(String.format("Group %s is not defined.", group));
        }

        for (Map.Entry<String, List<String>> entry : tmpGroups.get().entrySet()) {
            if (entry.getValue().contains(property.getKey())) {
                throw new RuntimeException(
                        String.format("Property %s is already added to group  %s.", property.getKey(), group));
            }
        }

        int order = tmpGroups.get().get(group).size();

        tmpConfig.get().define(property.getKey(), getConfigDefType(javaType), property.getDefaultValue(),
                property.getImportance(), property.getDoc(), group, order, property.getWidth(), property.getDisplay());

        tmpGroups.get().get(group).add(property.getKey());
    }

    public AvroData constructAvroData() {
        int schemaCacheSize = getProperty(SCHEMA_CACHE_SIZE, Integer.class);
        return new AvroData(schemaCacheSize);
    }

    private static ConfigDef.Type getConfigDefType(Class<?> javaType) {
        if (javaType == null) {
            return null;
        }
        switch (javaType.getSimpleName()) {
        case "Double":
        case "Float":
            return ConfigDef.Type.DOUBLE;
        case "Integer":
            return ConfigDef.Type.INT;
        case "Long":
            return ConfigDef.Type.LONG;
        case "String":
            return ConfigDef.Type.STRING;
        case "Boolean":
            return ConfigDef.Type.BOOLEAN;
        default:
            throw new RuntimeException("Unknown config type for java type " + javaType.getSimpleName());
        }
    }

    public <T> T getProperty(WorkerProperty<T> property, Class<T> valueClz) {
        Object value = this.get(property.getKey());
        return valueClz.cast(value);
    }

    public ConnectorConfiguration(ConfigDef configDef, Map<String, String> props) {
        super(configDef, props);
    }

}
