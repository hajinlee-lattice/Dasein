package com.latticeengines.cache.exposed.service;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cache.CacheNames;
import com.latticeengines.domain.exposed.cache.CacheType;

@Component("cacheService")
public abstract class CacheServiceBase implements CacheService {

    private static String cacheTypeStr;

    private static Map<CacheType, CacheService> registry = new HashMap<>();

    protected CacheServiceBase(CacheType cacheType) {
        registry.put(cacheType, this);
    }

    @Value("${cache.type}")
    public void setCacheType(String cacheTypeStr) {
        CacheServiceBase.cacheTypeStr = cacheTypeStr;
    }

    public static CacheService getCacheService() {
        if (cacheTypeStr == null) {
            return registry.get(CacheType.Local);
        }
        CacheType cacheType = CacheType.getByCacheType(cacheTypeStr);

        if (cacheType == null) {
            throw new NullPointerException("Unknown cache type " + cacheType);
        }

        return registry.get(cacheType);
    }

    public abstract void refreshKeysByPattern(String pattern, CacheNames... cacheNames);

}
