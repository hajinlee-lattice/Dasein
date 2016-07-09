package com.latticeengines.datafabric.connector;

import org.apache.kafka.common.config.ConfigDef;

public class WorkerProperty<T> {

    private String key;
    private String doc;
    private String display;
    private String configGroup;
    private T defaultValue;

    private ConfigDef.Importance importance = ConfigDef.Importance.HIGH;
    private ConfigDef.Width width = ConfigDef.Width.MEDIUM;
    private ConfigDef.Validator validator;

    public WorkerProperty(String key, String doc, String display) {
        this.setKey(key);
        this.setDoc(doc);
        this.setDisplay(display);
    }

    public String getKey() {
        return key;
    }

    public WorkerProperty<T> setKey(String key) {
        this.key = key;
        return this;
    }

    public String getDoc() {
        return doc;
    }

    public WorkerProperty<T> setDoc(String doc) {
        this.doc = doc;
        return this;
    }

    public String getDisplay() {
        return display;
    }

    public WorkerProperty<T> setDisplay(String display) {
        this.display = display;
        return this;
    }

    public String getConfigGroup() {
        return configGroup;
    }

    public WorkerProperty<T> setConfigGroup(String configGroup) {
        this.configGroup = configGroup;
        return this;
    }

    public T getDefaultValue() {
        return defaultValue;
    }

    public WorkerProperty<T> setDefaultValue(T defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }

    public ConfigDef.Importance getImportance() {
        return importance;
    }

    public WorkerProperty<T> setImportance(ConfigDef.Importance importance) {
        this.importance = importance;
        return this;
    }

    public ConfigDef.Width getWidth() {
        return width;
    }

    public WorkerProperty<T> setWidth(ConfigDef.Width width) {
        this.width = width;
        return this;
    }

    public ConfigDef.Validator getValidator() {
        return validator;
    }

    public WorkerProperty<T> setValidator(ConfigDef.Validator validator) {
        this.validator = validator;
        return this;
    }
}
