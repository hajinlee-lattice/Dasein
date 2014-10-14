package com.latticeengines.camille.config;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.latticeengines.domain.exposed.camille.Path;

public class PropertiesManager<T> {

    public interface PropertyListener {
        public void propertyChanged(String name, Object value);
    }

    private Map<String, Object> properties = new ConcurrentHashMap<String, Object>();
    private Collection<PropertyListener> listeners = new CopyOnWriteArrayList<PropertyListener>();

    public PropertiesManager(T scope, Path path) {

    }

    public Object getProperty(String name) {
        return properties.get(name);
    }

    public void setProperty(String name, Object value) {
        properties.put(name, value);
        for (PropertyListener listener : listeners) {
            listener.propertyChanged(name, value);
        }
    }
}
