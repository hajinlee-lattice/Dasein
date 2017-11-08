package com.latticeengines.domain.exposed.cache;

import java.util.HashMap;
import java.util.Map;

public enum CacheType {

    Local("local"), //
    Redis("redis"), //
    Composite("composite");

    private final String cacheType;
    private static Map<String, CacheType> map = new HashMap<>();

    static {
        for (CacheType c : CacheType.values()) {
            map.put(c.cacheType, c);
        }
    }

    CacheType(String cacheType) {
        this.cacheType = cacheType;
    }

    public static CacheType getByCacheType(String cacheType) {
        return map.get(cacheType);
    }
}
