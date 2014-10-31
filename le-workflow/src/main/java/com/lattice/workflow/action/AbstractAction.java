package com.lattice.workflow.action;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public abstract class AbstractAction<E, T> {
    String OOZIE_ACTION_OUTPUT_PROPERTIES = "oozie.action.output.properties";

    abstract E getRequest(Map<String, String> argMap);

    abstract T execute(E request);

    abstract Properties getResponseProperties(T response);

    protected void doMain(String[] args) {

        LazySpringContext.autowireBean(this);

        Map<String, String> argMap = getArgMap(args);
        E request = getRequest(argMap);

        T response = execute(request);

        Properties responseProperties = getResponseProperties(response);

        writeReponseProperties(responseProperties);
    }

    private void writeReponseProperties(Properties responseProperties) {
        if (responseProperties == null || responseProperties.size() == 0) {
            return;
        }

        String oozieProp = System.getProperty(OOZIE_ACTION_OUTPUT_PROPERTIES);
        File propFile = new File(oozieProp);
        try (OutputStream os = new FileOutputStream(propFile)) {
            responseProperties.store(os, "");
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private Map<String, String> getArgMap(String[] args) {
        Map<String, String> argMap = new HashMap<>();
        if (args != null) {
            for (String arg : args) {
                String[] tokens = arg.split("=");
                if (tokens.length > 1) {
                    argMap.put(tokens[0], tokens[1]);
                } else {
                    argMap.put(tokens[0], "");
                }
            }
        }
        return argMap;
    }

    protected <D> D getMappedValue(String value, Class<D> clazz) {
        if (value == null) {
            return null;
        }
        try {
            return clazz.getDeclaredConstructor(String.class).newInstance(value);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
