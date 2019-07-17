package com.latticeengines.cache.exposed.redis.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.StringUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.latticeengines.cache.exposed.redis.CacheWriter;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.dataplatform.HasId;
import com.latticeengines.domain.exposed.pls.EntityListCache;
import com.latticeengines.domain.exposed.security.Tenant;

public abstract class CacheWriterImpl<T extends HasId<String>> implements CacheWriter<T> {

    private static final Map<Class<? extends Throwable>, Boolean> RETRY_EXCEPTIONS = new HashMap<>();

    private static final int BATCH_SIZE = 200;

    private static final String DELIMITER = ":";

    static {
        RETRY_EXCEPTIONS.put(RedisConnectionFailureException.class, true);
        RETRY_EXCEPTIONS.put(RedisSystemException.class, true);
    }

    @Value("${proxy.retry.initialwaitmsec:500}")
    private long initialWaitMsec;
    @Value("${proxy.retry.multiplier:2}")
    private double multiplier;
    @Value("${proxy.retry.maxattempts:5}")
    private int maxAttempts;
    @Value("${cache.redis.namespace}")
    private String namespace;
    @Value("${cache.redis.ids.ttl}")
    protected long idsTTL;
    @Value("${cache.redis.entity.ttl}")
    protected long entityTTL;

    @Autowired
    protected RedisTemplate redisTemplate;

    protected RetryTemplate retryTemplate;

    public CacheWriterImpl() {

    }

    protected abstract String getCacheKeyPrefix();

    protected abstract String getIdListKey();

    protected abstract String getObjectKey();

    protected abstract String getCacheLockKey();

    protected abstract void fixLazyField(T entity);

    /*
     * Cache tenant key format: {PREFIX}:{NAMESPACE}:$ID_LIST_KEY:$ID
     */
    protected String getCacheKeyForIds(Tenant tenant) {
        // only using pid for now
        return StringUtils.collectionToDelimitedString(
                Arrays.asList(getCacheKeyPrefix(), namespace, getIdListKey(), tenant.getPid()), DELIMITER);
    }

    /*
     * Cache real object key format: {PREFIX}:{NAMESPACE}:$ID_KEY:$ID
     */
    @Override
    public String getCacheKeyForObject(String id) {
        Preconditions.checkNotNull(id);
        return StringUtils.collectionToDelimitedString(Arrays.asList(getCacheKeyPrefix(), namespace, getObjectKey(), id)
                , DELIMITER);
    }

    @Override
    public String getLockKeyForObject(Tenant tenant) {
        return StringUtils.collectionToDelimitedString(Arrays.asList(getCacheKeyPrefix(), namespace,
                getCacheLockKey(), tenant.getPid())
                , DELIMITER);
    }

    protected void check(Tenant tenant) {
        Preconditions.checkNotNull(tenant.getPid());
    }

    @Override
    public List<T> getEntitiesByTenant(Tenant tenant) {
        check(tenant);
        List<String> ids = getIdsByTenant(tenant);
        return getEntitiesByIds(ids);
    }

    // some entities may not exist, for example update or create entity case
    @Override
    public EntityListCache<T> getEntitiesAndNonExistEntitityIdsByTenant(Tenant tenant) {
        check(tenant);
        List<String> ids = getIdsByTenant(tenant);
        List<List<String>> idsToGet = Lists.partition(ids, BATCH_SIZE);
        List<T> result = new ArrayList<>();
        Set<String> nonExistIds = new HashSet<>();
        idsToGet.forEach(subIds -> {
            Set<String> idSet = new HashSet<>(subIds);
            List<T> subResults = multiGet(subIds);
            List<T> subResultToReturn = new ArrayList<>();
            subResults.forEach(t -> {
                if (t != null) {
                    subResultToReturn.add(t);
                    idSet.remove(t.getId());
                }
            });
            nonExistIds.addAll(idSet);
            if (CollectionUtils.isNotEmpty(subResultToReturn)) {
                result.addAll(subResultToReturn);
            }
        });
        EntityListCache<T> entityListCache = new EntityListCache<>();
        entityListCache.setExistEntities(result);
        entityListCache.setNonExistIds(nonExistIds);
        return entityListCache;
    }

    private List<String> getIdsByTenant(Tenant tenant) {
        return retryTemplate.execute(ctx -> {
            final String key = getCacheKeyForIds(tenant);
            return redisTemplate.opsForList().range(key, 0, -1);
        });
    }

    @PostConstruct
    private void setup() {
        retryTemplate = RetryUtils.getExponentialBackoffRetryTemplate(
                maxAttempts, initialWaitMsec, multiplier, RETRY_EXCEPTIONS);
    }

    @Override
    public void deleteIdsByTenant(Tenant tenant) {
        retryTemplate.execute(ctx -> {
            final String key = getCacheKeyForIds(tenant);
            redisTemplate.delete(key);
            return null;
        });
    }

    @Override
    public void setEntitiesIdsByTenant(Tenant tenant, List<T> entities) {
        check(tenant);
        Preconditions.checkNotNull(entities);
        if (entities.isEmpty()) {
            return;
        }
        final String key = getCacheKeyForIds(tenant);
        List<String> ids = entities
                .stream()
                .map(T -> T.getId())
                .collect(Collectors.toList());
        setIds(ids, key);
    }

    @Override
    public void setIdsAndEntitiesByTenant(Tenant tenant, List<T> entities) {
        setEntitiesIdsByTenant(tenant, entities);
        setEntities(entities);
    }

    @Override
    public void setEntities(List<T> entities) {
        List<List<T>> entitiesToSet = Lists.partition(entities, BATCH_SIZE);
        entitiesToSet.forEach(subEntities -> multiSet(subEntities));
    }

    private void multiSet(List<T> entities) {
        retryTemplate.execute(ctx -> {
            RedisCallback<Object> redisCallback = (connection) -> {
                entities.forEach(value -> {
                    fixLazyField(value);
                    connection.set(redisTemplate.getKeySerializer().serialize(getCacheKeyForObject(value.getId())),
                            redisTemplate.getValueSerializer().serialize(value), Expiration.from(entityTTL,
                                    TimeUnit.SECONDS), RedisStringCommands.SetOption.UPSERT);
                });
                return null;
            };
            redisTemplate.executePipelined(redisCallback);
            return null;
        });
    }

    private List<T> multiGet(List<String> ids) {
        RedisCallback<T> redisCallback = (connection) -> {
            ids.forEach(value -> connection.get(redisTemplate.getKeySerializer().serialize(getCacheKeyForObject(value))));
            return null;
        };
        return retryTemplate.execute(ctx -> redisTemplate.executePipelined(redisCallback, redisTemplate.getValueSerializer()));
    }

    private void multiGet(List<T> result, List<String> ids) {
        List<T> subResults = multiGet(ids);
        if (CollectionUtils.isNotEmpty(subResults)) {
            subResults.removeIf(Objects::isNull);
            result.addAll(subResults);
        }
    }

    @Override
    public T getEntityById(String id) {
        String key = getCacheKeyForObject(id);
        return (T) redisTemplate.opsForValue().get(key);
    }

    @Override
    public List<T> getEntitiesByIds(List<String> ids) {
        List<List<String>> idsToGet = Lists.partition(ids, BATCH_SIZE);
        List<T> result = new ArrayList<>();
        idsToGet.forEach(subIds -> {
            multiGet(result, subIds);
        });
        return result;
    }

    private void multiDelete(List<String> ids) {
        RedisCallback<Object> redisCallback = (connection) -> {
            ids.forEach(value -> connection.del(redisTemplate.getKeySerializer().serialize(getCacheKeyForObject(value))));
            return null;
        };
        retryTemplate.execute(ctx -> redisTemplate.executePipelined(redisCallback));
    }

    @Override
    public void deleteIdsAndEntitiesByTenant(Tenant tenant) {
        check(tenant);
        List<String> ids = getIdsByTenant(tenant);
        deleteIdsByTenant(tenant);
        deleteEntitiesByIds(ids);
    }

    @Override
    public void deleteEntitiesByIds(List<String> ids) {
        List<List<String>> idsToGet = Lists.partition(ids, BATCH_SIZE);
        idsToGet.forEach(subIds -> multiDelete(subIds));
    }

    @Override
    public void setEntitiesIdsAndNonExistEntitiesByTenant(Tenant tenant, List<T> entities) {
        final String key = getCacheKeyForIds(tenant);
        List<String> ids = new ArrayList<>();
        Map<String, T> entitityMap = new HashMap<>();
        entities.forEach(t -> {
            String id = t.getId();
            entitityMap.put(id, t);
            ids.add(id);
        });
        setIds(ids, key);
        List<List<String>> idsToGet = Lists.partition(ids, BATCH_SIZE);
        List<T> nonExistEntities;
        idsToGet.forEach(subIds -> {
            List<T> subResults = multiGet(subIds);
            subResults.forEach(t -> {
                if (t != null) {
                    entitityMap.remove(t.getId());
                }
            });
        });
        nonExistEntities = new ArrayList<>(entitityMap.values());
        if (CollectionUtils.isNotEmpty(nonExistEntities)) {
            setEntities(nonExistEntities);
        }
    }

    private void setIds(List<String> ids, String key) {
        RedisCallback<Object> redisCallback = (connection) -> {
            ids.forEach(id -> connection.rPush(redisTemplate.getKeySerializer().serialize(key),
                    redisTemplate.getValueSerializer().serialize(id)));
            return null;
        };
        retryTemplate.execute(ctx -> {
            redisTemplate.executePipelined(redisCallback);
            redisTemplate.expire(key, idsTTL, TimeUnit.SECONDS);
            return null;
        });
    }
}
