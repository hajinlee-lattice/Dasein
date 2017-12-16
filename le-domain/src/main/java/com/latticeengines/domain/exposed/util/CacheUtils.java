package com.latticeengines.domain.exposed.util;

import com.latticeengines.domain.exposed.cache.operation.CacheOperation;

public class CacheUtils {

    public static String getKeyOperation(CacheOperation op, String key) {
        return String.format("%d|%s|key|%s", System.currentTimeMillis(), op.name(), key);
    }

    public static String getAllOperation(CacheOperation op, String key) {
        return String.format("%d|%s|all|%s", System.currentTimeMillis(), op.name(), key);
    }
}

