package com.latticeengines.propdata.collection.service;

import java.util.HashMap;
import java.util.Map;

import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgressBase;

public class CollectionJobContext {

    public static final String PROGRESS_KEY = "Progress";
    public static final String APPLICATIONID_KEY = "ApplicationId";


    public static <T extends ArchiveProgressBase> CollectionJobContext constructFromProgress(T progress) {
        if (progress == null) {
            return CollectionJobContext.NULL;
        } else {
            CollectionJobContext context = new CollectionJobContext();
            context.setProperty(CollectionJobContext.PROGRESS_KEY, progress);
            return context;
        }
    }

    private Map<String, Object> properties = new HashMap<String, Object>();

    public void setProperty(String name, Object value) {
        properties.put(name, value);
    }

    public <T> T getProperty(String name, Class<T> valueClass) {
        Object value = properties.get(name);

        if (value == null) {
            return null;
        }

        if (valueClass.isInstance(value)) {
            return valueClass.cast(value);
        } else {
            throw new RuntimeException("Value is not of type " + valueClass);
        }
    }

    public Map<String, Object> getMap() {
        return properties;
    }

    public static final CollectionJobContext NULL = new CollectionJobContext();

}
