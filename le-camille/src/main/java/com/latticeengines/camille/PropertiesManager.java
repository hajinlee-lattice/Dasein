package com.latticeengines.camille;

import java.util.HashMap;
import java.util.Map;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;

public class PropertiesManager {

    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    private static final ObjectMapper mapper = new ObjectMapper();

    private CamilleCache cache;
    private Path path;

    public PropertiesManager(Path path) throws Exception {
        cache = new CamilleCache(this.path = path);
        cache.start();
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

    // ////////////////////////////////////////////////////////////////////////////////

    // private methods

    @SuppressWarnings("unchecked")
    private <T> T getProperty(String name, Class<T> clazz) throws Exception {
        try {
            cache.rebuild();
            Map<String, Object> map = mapper.readValue(cache.get().getData(), new TypeReference<Map<String, Object>>() {
            });
            return (T) map.get(name);
        } catch (Exception e) {
            log.error("Error reading properties data", e);
            throw e;
        }
    }

    /**
     * Helper method for setProperty
     */
    private void doSetProperty(String name, Object value) throws Exception {
        Document doc = cache.get();

        String data = doc.getData();

        @SuppressWarnings("unchecked")
        Map<String, Object> dict = data == null || data.isEmpty() ? new HashMap<String, Object>() : mapper.readValue(
                doc.getData(), HashMap.class);

        dict.put(name, value);
        doc.setData(mapper.writeValueAsString(dict));

        CamilleEnvironment.getCamille().set(path, doc);
    }

    private void setProperty(String name, Object value) throws Exception {
        try {
            try {
                doSetProperty(name, value);
            } catch (KeeperException.BadVersionException e) {
                cache.rebuild();
                doSetProperty(name, value);
            }
        } catch (Exception e) {
            log.error("Error writing properties data", e);
            throw e;
        }
    }
}
