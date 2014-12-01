package com.latticeengines.camille.properties;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.camille.config.ConfigurationController;
import com.latticeengines.camille.config.cache.ConfigurationCache;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;

public class PropertiesManager<T extends ConfigurationScope> {

    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    protected static final ObjectMapper mapper = new ObjectMapper();

    protected final ConfigurationCache<T> configCache;
    protected final ConfigurationController<T> configController;
    protected final Path relativePath;

    public PropertiesManager(T scope, Path relativePath) throws Exception {
        configController = new ConfigurationController<T>(scope);
        configCache = new ConfigurationCache<T>(scope, this.relativePath = relativePath);
    }

    public String getStringProperty(String name) throws Exception {
        return getProperty(name, String.class);
    }

    public void setStringProperty(String name, String value) throws Exception {
        setProperty(name, value);
    }

    public double getDoubleProperty(String name) throws Exception {
        return getProperty(name, Double.class);
    }

    public void setDoubleProperty(String name, double value) throws Exception {
        setProperty(name, value);
    }

    public int getIntProperty(String name) throws Exception {
        return getProperty(name, Integer.class);
    }

    public void setIntProperty(String name, int value) throws Exception {
        setProperty(name, value);
    }

    public boolean getBooleanProperty(String name) throws Exception {
        return getProperty(name, Boolean.class);
    }

    @SuppressWarnings("unchecked")
    protected <P> P getProperty(String name, Class<P> clazz) throws Exception {
        try {
            Document doc = configCache.get();
            if (doc == null)
                doc = new Document();
            return (P) getMap(doc.getData()).get(name);
        } catch (Exception e) {
            log.error("Error reading properties data", e);
            throw e;
        }
    }

    protected void setProperty(String name, Object value) throws Exception {
        try {
            try {
                doSetProperty(name, value);
            } catch (KeeperException.BadVersionException e) {
                configCache.rebuild();
                doSetProperty(name, value);
            }
        } catch (Exception e) {
            log.error("Error writing properties data", e);
            throw e;
        }
    }

    /**
     * Helper method for setProperty
     */
    private void doSetProperty(String name, Object value) throws Exception {
        Document doc = configCache.get();
        if (doc == null)
            doc = new Document();
        Map<String, Object> map = getMap(doc.getData());
        map.put(name, value);
        doc.setData(mapper.writeValueAsString(map));
        try {
            configController.set(relativePath, doc);
        } catch (KeeperException.NoNodeException e) {
            configController.create(relativePath, doc);
        }
        configCache.rebuild();
    }

    @SuppressWarnings("unchecked")
    private <M extends Map<String, Object>> M getMap(String data) throws JsonParseException, JsonMappingException,
            IOException {
        return (M) (data == null || data.isEmpty() ? new HashMap<String, Object>() : mapper.readValue(data,
                new TypeReference<Map<String, Object>>() {
                }));
    }
}
