package com.latticeengines.workflow.exposed.build;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Pass non-serialized objects among workflow steps
 * Since they are not serialized, so cannot be persisted (for retry)
 */
public final class WorkflowStaticContext {

    public static final String ATTRIBUTE_REPO = "ATTRIBUTE_REPO";
    public static final String EXPORT_SCHEMA_MAP = "EXPORT_SCHEMA_MAP";
    public static final String ATLAS_EXPORT = "ATLAS_EXPORT";

    private static final ConcurrentMap<String, Object> contextMap = new ConcurrentHashMap<>();

    public static <T>  T getObject(String key, Class<T> clz) {
        Object obj = contextMap.get(key);
        if (obj != null) {
            return clz.cast(obj);
        } else {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    public static <K, V> Map<K, V> getMapObject(String key, Class<K> keyClz, Class<V> valClz) {
        Map map = getObject(key, Map.class);
        if (map == null) {
            return null;
        } else {
            return (Map<K, V>) map;
        }
    }

    public static void putObject(String key, Object obj) {
        contextMap.put(key, obj);
    }

}
