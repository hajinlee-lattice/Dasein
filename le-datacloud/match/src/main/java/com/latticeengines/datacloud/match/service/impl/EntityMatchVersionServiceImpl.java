package com.latticeengines.datacloud.match.service.impl;

import static com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder.N;
import static com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder.attribute_not_exists;

import java.util.Arrays;
import java.util.ConcurrentModificationException;
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
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
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
    private static final String ATTR_NEXT_VERSION = DataCloudConstants.ENTITY_ATTR_NEXT_VERSION;
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
    public int getNextVersion(@NotNull EntityMatchEnvironment environment, @NotNull Tenant tenant) {
        tenant = EntityMatchUtils.newStandardizedTenant(tenant);
        initCache();

        PrimaryKey key = buildKey(environment, tenant);
        return getNextVersion(dynamoItemService.getItem(tableName, key));
    }

    @Override
    public int bumpVersion(@NotNull EntityMatchEnvironment environment, @NotNull Tenant tenant) {
        return bumpVersion(environment, tenant, true);
    }

    @Override
    public int bumpNextVersion(EntityMatchEnvironment environment, Tenant tenant) {
        return bumpVersion(environment, tenant, false);
    }

    /*-
     * if bumpBothVersions is true, set current version to next version and incr next version by 1
     * otherwise only incr next version by 1
     */
    private int bumpVersion(@NotNull EntityMatchEnvironment environment, @NotNull Tenant tenant,
            boolean bumpBothVersions) {
        tenant = EntityMatchUtils.newStandardizedTenant(tenant);
        initCache();
        PrimaryKey key = buildKey(environment, tenant);
        Item item = dynamoItemService.getItem(tableName, key);
        int currVersion = item == null ? DEFAULT_VERSION : item.getInt(ATTR_VERSION);
        int nextVersion = getNextVersion(item);
        UpdateItemExpressionSpec updateSpec = optimisticLocking(new ExpressionSpecBuilder(), item) //
                .addUpdate(N(ATTR_VERSION).set(bumpBothVersions ? nextVersion : currVersion)) //
                .addUpdate(N(ATTR_NEXT_VERSION).set(nextVersion + 1)) //
                .buildForUpdate();
        return updateWithSpec(updateSpec, key, tenant, environment,
                bumpBothVersions ? ATTR_VERSION : ATTR_NEXT_VERSION);
    }

    @Override
    public void setVersion(@NotNull EntityMatchEnvironment environment, @NotNull Tenant tenant, int version) {
        tenant = EntityMatchUtils.newStandardizedTenant(tenant);
        initCache();
        PrimaryKey key = buildKey(environment, tenant);
        Item item = dynamoItemService.getItem(tableName, key);
        int nextVersion = getNextVersion(item);
        if (version < DEFAULT_VERSION || version >= nextVersion) {
            throw new IllegalArgumentException(
                    String.format("Version must be within [%d,%d)", DEFAULT_VERSION, nextVersion));
        }

        UpdateItemExpressionSpec updateSpec = optimisticLocking(new ExpressionSpecBuilder(), item) //
                .addUpdate(N(ATTR_VERSION).set(version)) //
                .buildForUpdate();
        updateWithSpec(updateSpec, key, tenant, environment, ATTR_VERSION);
    }

    @Override
    public void invalidateCache(@NotNull Tenant tenant) {
        tenant = EntityMatchUtils.newStandardizedTenant(tenant);
        initCache();

        String tenantId = tenant.getId();
        Arrays.stream(EntityMatchEnvironment.values()).forEach(env -> {
            Pair<String, EntityMatchEnvironment> cacheKey = Pair.of(tenantId, env);
            versionCache.invalidate(cacheKey);
        });
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

    private int updateWithSpec(@NotNull UpdateItemExpressionSpec updateSpec, @NotNull PrimaryKey key,
            @NotNull Tenant tenant, @NotNull EntityMatchEnvironment environment, @NotNull String returnCol) {
        try {
            UpdateItemSpec spec = new UpdateItemSpec().withPrimaryKey(key) //
                    .withExpressionSpec(updateSpec) //
                    .withReturnValues(ReturnValue.UPDATED_NEW); // get the updated version back
            UpdateItemOutcome result = dynamoItemService.update(tableName, spec);
            Preconditions.checkNotNull(result);
            Preconditions.checkNotNull(result.getItem());
            // clear cache
            versionCache.invalidate(Pair.of(tenant.getId(), environment));
            return result.getItem().getInt(returnCol);
        } catch (ConditionalCheckFailedException e) {
            throw new ConcurrentModificationException(e);
        }
    }

    private ExpressionSpecBuilder optimisticLocking(@NotNull ExpressionSpecBuilder builder, Item item) {
        // optimistic locking
        if (item == null) {
            builder.withCondition(attribute_not_exists(ATTR_VERSION));
        } else {
            builder.withCondition(N(ATTR_VERSION).eq(item.getInt(ATTR_VERSION)));
        }
        return builder;
    }

    private int getNextVersion(Item item) {
        if (item == null) {
            return DEFAULT_VERSION + 1;
        } else if (!item.hasAttribute(ATTR_NEXT_VERSION)) {
            // for backward compatibility, need to check whether next version attr exist
            return item.getInt(ATTR_VERSION) + 1;
        }
        return item.getInt(ATTR_NEXT_VERSION);
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
