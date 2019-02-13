package com.latticeengines.datacloud.match.service.impl;

import static com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder.N;

import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder;
import com.amazonaws.services.dynamodbv2.xspec.UpdateItemExpressionSpec;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.latticeengines.aws.dynamo.DynamoItemService;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.service.EntityMatchConfigurationService;
import com.latticeengines.datacloud.match.service.EntityMatchVersionService;
import com.latticeengines.datacloud.match.util.EntityMatchUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("entityMatchVersionService")
public class EntityMatchVersionServiceImpl implements EntityMatchVersionService {

    /* constants */
    private static final String PREFIX = DataCloudConstants.ENTITY_PREFIX_VERSION;
    private static final String ATTR_PARTITION_KEY = DataCloudConstants.ENTITY_ATTR_PID;
    private static final String ATTR_VERSION = DataCloudConstants.ENTITY_ATTR_VERSION;
    private static final String DELIMITER = DataCloudConstants.ENTITY_DELIMITER;
    // treat null item as version 0 because dynamo treat it this way (ADD on null item will result in 1)
    private static final int DEFAULT_VERSION = 0;
    // use the limit just in case, will probably never exceeds this and the memory used by this cache is small anyways
    private static final long MAX_CACHED_ENTRIES = 2000L;

    private final DynamoItemService dynamoItemService;
    private volatile Cache<Pair<String, EntityMatchEnvironment>, Integer> versionCache;
    @Value("${datacloud.match.entity.cache.version.ttl:1800}")
    private long cacheTTLInSeconds;
    private String tableName; // currently store both version in serving for faster lookup

    @Inject
    public EntityMatchVersionServiceImpl(
            DynamoItemService dynamoItemService, EntityMatchConfigurationService entityMatchConfigurationService) {
        this.dynamoItemService = dynamoItemService;
        // NOTE this will not be changed at runtime
        this.tableName = entityMatchConfigurationService.getTableName(EntityMatchEnvironment.SERVING);
    }

    @Override
    public int getCurrentVersion(@NotNull EntityMatchEnvironment environment, @NotNull Tenant tenant) {
        tenant = EntityMatchUtils.newStandardizedTenant(tenant);
        initCache();
        Pair<String, EntityMatchEnvironment> cacheKey = Pair.of(tenant.getId(), environment);
        // try in memory cache first
        Integer version = versionCache.getIfPresent(cacheKey);
        if (version != null) {
            return version;
        }

        // try dynamo and update cache
        PrimaryKey key = buildKey(environment, tenant);
        Item item = dynamoItemService.getItem(tableName, key);
        version = item == null ? DEFAULT_VERSION : item.getInt(ATTR_VERSION);
        versionCache.put(cacheKey, version);
        return version;
    }

    @Override
    public int bumpVersion(@NotNull EntityMatchEnvironment environment, @NotNull Tenant tenant) {
        tenant = EntityMatchUtils.newStandardizedTenant(tenant);
        initCache();
        PrimaryKey key = buildKey(environment, tenant);
        UpdateItemExpressionSpec expressionSpec = new ExpressionSpecBuilder()
                .addUpdate(N(ATTR_VERSION).add(1)) // increase by one
                .buildForUpdate();
        UpdateItemSpec spec = new UpdateItemSpec()
                .withPrimaryKey(key)
                .withExpressionSpec(expressionSpec)
                .withReturnValues(ReturnValue.UPDATED_NEW); // get the updated version back
        UpdateItemOutcome result = dynamoItemService.update(tableName, spec);
        Preconditions.checkNotNull(result);
        Preconditions.checkNotNull(result.getItem());
        // clear cache
        versionCache.invalidate(Pair.of(tenant.getId(), environment));
        return result.getItem().getInt(ATTR_VERSION);
    }

    @Override
    public void clearVersion(@NotNull EntityMatchEnvironment environment, @NotNull Tenant tenant) {
        tenant = EntityMatchUtils.newStandardizedTenant(tenant);
        initCache();
        PrimaryKey key = buildKey(environment, tenant);
        dynamoItemService.delete(tableName, new DeleteItemSpec().withPrimaryKey(key));
        // clear cache
        versionCache.invalidate(Pair.of(tenant.getId(), environment));
    }

    @VisibleForTesting
    void setTableName(@NotNull String tableName) {
        Preconditions.checkNotNull(tableName);
        this.tableName = tableName;
    }

    @VisibleForTesting
    Cache<Pair<String, EntityMatchEnvironment>, Integer> getVersionCache() {
        return versionCache;
    }

    /*
     * instantiate version cache lazily (on first request)
     */
    private void initCache() {
        if (versionCache != null) {
            return;
        }

        synchronized (this) {
            if (versionCache == null) {
                versionCache = Caffeine
                        .newBuilder()
                        // TTL starts after writes instead of access, limit the staleness of data just in case
                        .expireAfterWrite(cacheTTLInSeconds, TimeUnit.SECONDS)
                        .maximumSize(MAX_CACHED_ENTRIES)
                        .build();
            }
        }
    }

    /*
     * Build dynamo key, schema:
     * - Partition Key: <PREFIX>_<ENV>_<TENANT_PID>   E.g., "VERSION_STAGING_123"
     */
    private PrimaryKey buildKey(@NotNull EntityMatchEnvironment environment, @NotNull Tenant tenant) {
        Preconditions.checkNotNull(environment);
        Preconditions.checkNotNull(tenant);
        Preconditions.checkNotNull(tenant.getId());

        String partitionKey = String.join(DELIMITER, PREFIX, environment.name(), tenant.getId());
        return new PrimaryKey(ATTR_PARTITION_KEY, partitionKey);
    }
}
