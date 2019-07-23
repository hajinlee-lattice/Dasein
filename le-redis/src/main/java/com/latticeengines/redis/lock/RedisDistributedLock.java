package com.latticeengines.redis.lock;

public interface RedisDistributedLock {

    boolean lock(String key, String requestId, long expire, boolean retry);

    boolean releaseLock(String key, String requestId);

    String get(String key);
}
