package com.latticeengines.cache.exposed.redis.impl;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.google.common.base.Preconditions;
import com.latticeengines.cache.exposed.redis.CacheService;
import com.latticeengines.cache.exposed.redis.CacheWriter;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.dataplatform.HasId;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.EntityListCache;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.redis.lock.RedisDistributedLock;

public abstract class BaseCacheServiceImpl<T extends HasId<String>> implements CacheService<T> {

    private static final Logger log = LoggerFactory.getLogger(BaseCacheServiceImpl.class);

    @Value("${cache.lock.build.seconds}")
    private long cacheBuild;
    @Value("${cache.lock.clear.seconds}")
    private long cacheClear;

    private static final int NUM_THREADS = 8;

    @Autowired
    private RedisDistributedLock redisDistributedLock;

    private ExecutorService service = ThreadPoolUtils.getFixedSizeThreadPool(getThreadPoolName(), NUM_THREADS);

    protected abstract CacheWriter<T> getCacheWriter();

    protected abstract String getThreadPoolName();

    protected abstract List<T> getAll();

    protected abstract List<T> findEntitiesByIds(Set<String> ids);

    @Override
    public T getEntityById(String id) {
        return getCacheWriter().getEntityById(id);
    }

    @Override
    public List<T> getEntitiesByTenant(Tenant tenant) {
        return getCacheWriter().getEntitiesByTenant(tenant);
    }

    @Override
    public void deleteIdsByTenant(Tenant tenant) {
        log.info("Delete entities by tenant: %s", tenant);
        getCacheWriter().deleteIdsByTenant(tenant);
    }

    @Override
    public void deleteIdsAndEntitiesByTenant(Tenant tenant) {
        getCacheWriter().deleteIdsAndEntitiesByTenant(tenant);
    }

    @Override
    public void setIdsAndEntitiesByTenant(Tenant tenant, List<T> entities) {
        getCacheWriter().setIdsAndEntitiesByTenant(tenant, entities);
    }

    @Override
    public List<T> getEntitiesByIds(List<String> ids) {
        return getCacheWriter().getEntitiesByIds(ids);
    }

    @Override
    public void deleteEntitiesByIds(List<String> ids) {
        log.info("Delete entities by ids: %s", ids);
        getCacheWriter().deleteEntitiesByIds(ids);
    }

    @Override
    public EntityListCache<T> getEntitiesAndNonExistEntitityIdsByTenant(Tenant tenant) {
        if (!StringUtils.isEmpty(redisDistributedLock.get(getCacheWriter().getLockKeyForObject(tenant)))) {
            throw new LedpException(LedpCode.LEDP_18227);
        }
        return getCacheWriter().getEntitiesAndNonExistEntitityIdsByTenant(tenant);
    }

    @Override
    public Future<?> buildEntitiesCache(Tenant tenant) {
        return service.submit(new RefreshCache(tenant));
    }

    private class RefreshCache implements Runnable {
        private final Tenant tenant;

        RefreshCache(Tenant tenant) {
            Preconditions.checkNotNull(tenant);
            this.tenant = tenant;
        }

        @Override
        public void run() {
            if (tenant == null) {
                log.info("Can't build cache since tenant is null.");
                return;
            }
            String key = getCacheWriter().getLockKeyForObject(tenant);
            String requestId = UUID.randomUUID().toString();
            if (redisDistributedLock.lock(key, requestId, cacheBuild, false)) {
                try {
                    log.info("Start to build cache for tenant %s.", tenant.getPid());
                    MultiTenantContext.setTenant(tenant);
                    EntityListCache entityListCache = getCacheWriter().getEntitiesAndNonExistEntitityIdsByTenant(tenant);
                    List<T> entities = entityListCache.getExistEntities();
                    Set<String> nonExistIds = entityListCache.getNonExistIds();
                    List<T> nonExistEntities;
                    if (entities.size() == 0) {
                        // build all cache
                        List<T> allEntities = getAll();
                        getCacheWriter().setEntitiesIdsAndNonExistEntitiesByTenant(tenant, allEntities);
                    } else if (nonExistIds.size() > 0) {
                        // only get non exist entities
                        nonExistEntities = findEntitiesByIds(nonExistIds);
                        getCacheWriter().setEntities(nonExistEntities);
                    }
                    MultiTenantContext.setTenant(null);
                } catch (LedpException ledpException) {
                    // some other threads get the new locks
                } finally {
                    redisDistributedLock.releaseLock(key, requestId);
                }
            }
        }

    }

    @Override
    public Future<?> clearCache(Tenant tenant, String entityId) {
        return service.submit(new ClearCache(tenant, entityId));
    }

    private class ClearCache implements Runnable {
        private final Tenant tenant;
        private final String entityId;

        ClearCache(Tenant tenant, String entityId) {
            Preconditions.checkNotNull(tenant);
            this.tenant = tenant;
            this.entityId = entityId;
        }

        @Override
        public void run() {
            String key = getCacheWriter().getLockKeyForObject(tenant);
            String requestId = UUID.randomUUID().toString();
            if (redisDistributedLock.lock(key, requestId, cacheClear, true)) {
                try {
                    deleteData();
                } finally {
                    redisDistributedLock.releaseLock(key, requestId);
                }
            } else {
                // can't get lock
                try {
                    Thread.sleep(500);
                    service.submit(this);
                } catch (InterruptedException e) {
                    // ignore it
                    deleteData();
                }
            }
        }

        private void deleteData() {
            getCacheWriter().deleteIdsByTenant(tenant);
            if (StringUtils.isNotBlank(entityId)) {
                getCacheWriter().deleteEntitiesByIds(Collections.singletonList(entityId));
            }
        }
    }

}
