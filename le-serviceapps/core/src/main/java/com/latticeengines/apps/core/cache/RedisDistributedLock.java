package com.latticeengines.apps.core.cache;

public interface RedisDistributedLock {

    boolean lock(String key, String requestId, boolean retry);

    boolean releaseLock(String key, String requestId);

    String get(String key);
}
