package com.latticeengines.domain.exposed.metadata;

import java.util.ArrayList;
import java.util.List;

public abstract class BaseMetadataPropertyOwner<P extends MetadataProperty> implements HasProperties<P> {

    public abstract List<P> getProperties();

    protected abstract void setProperties(List<P> properties);

    protected abstract Class<P> getPropertyClz();

    @Override
    public void putProperty(String key, String value) {
        boolean updated = false;
        List<P> newProps = new ArrayList<>();
        for (P prop : getProperties()) {
            if (prop.getOption().equals(key)) {
                prop.setValue(value);
                updated = true;
            }
            newProps.add(prop);
        }
        if (!updated) {
            try {
                P prop = getPropertyClz().newInstance();
                prop.setOption(key);
                prop.setValue(value);
                newProps.add(prop);
            } catch (InstantiationException | IllegalAccessException e) {
                throw new RuntimeException(
                        String.format("Failed to instantiate property from key=%s, value=%s", key, value));
            }
        }
        setProperties(newProps);
    }

    @Override
    public P getProperty(String key) {
        for (P property : getProperties()) {
            if (property.getProperty().equals(key)) {
                return property;
            }
        }
        return null;
    }

}
