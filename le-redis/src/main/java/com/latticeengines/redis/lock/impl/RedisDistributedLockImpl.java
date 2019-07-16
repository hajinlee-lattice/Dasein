package com.latticeengines.redis.lock.impl;

import java.util.Collections;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import com.latticeengines.redis.lock.RedisDistributedLock;

@Component("redisDistributedLock")
public class RedisDistributedLockImpl implements RedisDistributedLock {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    private final Long SUCCESS = 1L;

    private RedisScript<Long> lockScript;

    private RedisScript<Long> unlockScript;

    private RedisScript<String> getLockScript;
    
    @Value("${common.redis.lock.maxattempts}")
    private int maxAttempts;
    @Value("${common.redis.lock.sleepinterval}")
    private int sleepInterval;

    @PostConstruct
    public void setupLuaScript() {
        String GET_LOCK_LUA = "return redis.call('get',KEYS[1])";
        String LOCK_LUA = "if redis.call('set',KEYS[1],ARGV[1],'NX','PX',ARGV[2]) then return 1 else return 0 end";
        String UNLOCK_LUA = "if redis.call('get',KEYS[1])==ARGV[1] then return redis.call('del',KEYS[1]) else return" +
                " 0 end";
        lockScript = new DefaultRedisScript<>(LOCK_LUA, Long.class);
        unlockScript = new DefaultRedisScript<>(UNLOCK_LUA, Long.class);
        getLockScript = new DefaultRedisScript<>(GET_LOCK_LUA, String.class);
    }

    private final Logger logger = LoggerFactory.getLogger(RedisDistributedLockImpl.class);

    @Override
    public boolean lock(String key, String requestId, long expire, boolean retry) {
        if (retry) {
            int maxAttempts = this.maxAttempts;
            boolean result = setRedis(key, requestId, expire, true);
            while ((!result) && maxAttempts-- > 0) {
                try {
                    Thread.sleep(sleepInterval);
                } catch (InterruptedException e) {
                    return false;
                }
                result = setRedis(key, requestId, expire, true);
            }
            return result;
        } else {
            return setRedis(key, requestId, expire, true);
        }
    }

    private boolean setRedis(String key, String requestId, long expire, boolean lock) {
        try {
            Object result;
            if (lock) {
                result = redisTemplate.execute(lockScript, Collections.singletonList(key),
                        requestId,
                        expire);
            } else {
                result = redisTemplate.execute(unlockScript, Collections.singletonList(key),
                        requestId);
            }
            if (SUCCESS.equals(result)) {
                return true;
            }
        } catch (Exception e) {
            logger.error("Exception happened while setting redis lock: ", e);
        }
        return false;
    }

    @Override
    public String get(String key) {
        try {
            String result = redisTemplate.execute(getLockScript, Collections.singletonList(key));
            return result;
        } catch (Exception e) {
            logger.error("Exception happened while getting redis lock value: ", e);
        }
        return "";
    }

    @Override
    public boolean releaseLock(String key, String requestId) {
        try {
            return setRedis(key, requestId, -1l, false);
        } catch (Exception e) {
            logger.error("Exception happened while releasing redis lock: ", e);
        } finally {
        }
        return false;
    }

}
