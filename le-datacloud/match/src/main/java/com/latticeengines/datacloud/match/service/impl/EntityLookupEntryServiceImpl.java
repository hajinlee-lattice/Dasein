package com.latticeengines.datacloud.match.service.impl;

import static com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder.S;
import static com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder.attribute_not_exists;
import static com.latticeengines.common.exposed.util.ValidationUtils.checkNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder;
import com.amazonaws.services.dynamodbv2.xspec.PutItemExpressionSpec;
import com.latticeengines.aws.dynamo.DynamoItemService;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.service.EntityLookupEntryService;
import com.latticeengines.datacloud.match.service.EntityMatchConfigurationService;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("entityLookupEntryService")
public class EntityLookupEntryServiceImpl implements EntityLookupEntryService {

    /* constants */
    private static final String PREFIX = DataCloudConstants.ENTITY_PREFIX_LOOKUP;
    private static final String ATTR_PARTITION_KEY = DataCloudConstants.ENTITY_ATTR_PID;
    private static final String ATTR_RANGE_KEY = DataCloudConstants.ENTITY_ATTR_SID;
    private static final String ATTR_SEED_ID = DataCloudConstants.ENTITY_ATTR_SEED_ID;
    private static final String ATTR_EXPIRED_AT = DataCloudConstants.ENTITY_ATTR_EXPIRED_AT;
    private static final String DELIMITER = DataCloudConstants.ENTITY_DELIMITER;

    /* services */
    private final DynamoItemService dynamoItemService;
    private final EntityMatchConfigurationService entityMatchConfigurationService;
    private final int numStagingShards;

    @Inject
    public EntityLookupEntryServiceImpl(
            DynamoItemService dynamoItemService, EntityMatchConfigurationService entityMatchConfigurationService) {
        this.dynamoItemService = dynamoItemService;
        this.entityMatchConfigurationService = entityMatchConfigurationService;
        // NOTE this will not be changed at runtime
        numStagingShards = entityMatchConfigurationService.getNumShards(EntityMatchEnvironment.STAGING);
    }

    @Override
    public String get(
            @NotNull EntityMatchEnvironment env, @NotNull Tenant tenant, @NotNull EntityLookupEntry lookupEntry,
            int version) {
        checkNotNull(env, tenant, lookupEntry);
        PrimaryKey key = buildKey(env, tenant, lookupEntry, version);
        String tableName = getTableName(env);
        Item item = getRetryTemplate(env).execute(ctx -> dynamoItemService.getItem(tableName, key));
        return getSeedId(item);
    }

    @Override
    public List<String> get(
            @NotNull EntityMatchEnvironment env, @NotNull Tenant tenant, @NotNull List<EntityLookupEntry> lookupEntries,
            int version) {
        checkNotNull(env, tenant, lookupEntries);
        if (lookupEntries.isEmpty()) {
            return Collections.emptyList();
        }
        List<PrimaryKey> keys = lookupEntries
                .stream()
                .map(entry -> buildKey(env, tenant, entry, version))
                .collect(Collectors.toList());
        // dedup, batchGet does not allow duplicate entries
        Set<PrimaryKey> uniqueKeys = new HashSet<>(keys);
        Map<PrimaryKey, Item> itemMap = getRetryTemplate(env).execute(ctx -> {
            return dynamoItemService
                    .batchGet(getTableName(env), new ArrayList<>(uniqueKeys))
                    .stream()
                    .filter(Objects::nonNull)
                    .map(item -> Pair.of(getKey(env, item), item))
                    .collect(Collectors.toMap(Pair::getKey, Pair::getValue, (v1, v2) -> v1)); // ignore duplicates
        });
        return keys.stream().map(itemMap::get).map(this::getSeedId).collect(Collectors.toList());
    }

    @Override
    public boolean createIfNotExists(
            @NotNull EntityMatchEnvironment env, @NotNull Tenant tenant,
            @NotNull EntityLookupEntry lookupEntry, @NotNull String seedId, boolean setTTL, int version) {
        checkNotNull(env, tenant, lookupEntry, seedId);
        PutItemExpressionSpec expressionSpec = new ExpressionSpecBuilder()
                // use partition key to determine whether item exists
                .withCondition(attribute_not_exists(ATTR_PARTITION_KEY))
                .buildForPut();
        return conditionalSet(env, tenant, lookupEntry, seedId, expressionSpec, setTTL, version);
    }

    @Override
    public boolean setIfEquals(
            @NotNull EntityMatchEnvironment env, @NotNull Tenant tenant,
            @NotNull EntityLookupEntry lookupEntry, @NotNull String seedId, boolean setTTL, int version) {
        checkNotNull(env, tenant, lookupEntry, seedId);
        PutItemExpressionSpec expressionSpec = new ExpressionSpecBuilder()
                // use partition key to determine whether item exists
                .withCondition(attribute_not_exists(ATTR_SEED_ID).or(S(ATTR_SEED_ID).eq(seedId)))
                .buildForPut();
        return conditionalSet(env, tenant, lookupEntry, seedId, expressionSpec, setTTL, version);
    }

    @Override
    public void set(
            @NotNull EntityMatchEnvironment env, @NotNull Tenant tenant,
            @NotNull List<Pair<EntityLookupEntry, String>> pairs, boolean setTTL, int version) {
        checkNotNull(env, tenant, pairs);
        if (pairs.isEmpty()) {
            return;
        }

        long expiredAt = getExpiredAt();
        Map<PrimaryKey, String> seedIdMap = pairs
                .stream()
                .map(pair -> Pair.of(buildKey(env, tenant, pair.getKey(), version), pair.getValue()))
                .collect(Collectors.toMap(Pair::getLeft, Pair::getRight, (v1, v2) -> v1));
        List<Item> items = seedIdMap
                .entrySet()
                .stream()
                .map(entry -> buildItem(Pair.of(entry.getKey(), entry.getValue()), expiredAt, setTTL))
                .collect(Collectors.toList());

        getRetryTemplate(env).execute(ctx -> {
            // batch set
            dynamoItemService.batchWrite(getTableName(env), items);
            return null;
        });
    }

    @Override
    public boolean delete(
            @NotNull EntityMatchEnvironment env, @NotNull Tenant tenant, @NotNull EntityLookupEntry lookupEntry,
            int version) {
        checkNotNull(env, tenant, lookupEntry);
        PrimaryKey key = buildKey(env, tenant, lookupEntry, version);
        return getRetryTemplate(env).execute(ctx -> dynamoItemService.deleteItem(getTableName(env), key));
    }

    /*
     * Update lookup entry with condition specified in the given expression spec
     */
    private boolean conditionalSet(
            @NotNull EntityMatchEnvironment env, @NotNull Tenant tenant, @NotNull EntityLookupEntry lookupEntry,
            @NotNull String seedId, @NotNull PutItemExpressionSpec expressionSpec, boolean setTTL, int version) {
        PrimaryKey key = buildKey(env, tenant, lookupEntry, version);
        Item item = new Item()
                .withPrimaryKey(key)
                .withString(ATTR_SEED_ID, seedId);
        if (setTTL) {
            item.withNumber(ATTR_EXPIRED_AT, getExpiredAt());
        }

        PutItemSpec spec = new PutItemSpec()
                .withItem(item)
                .withExpressionSpec(expressionSpec)
                .withReturnValues(ReturnValue.NONE);

        try {
            getRetryTemplate(env).execute(ctx -> dynamoItemService.put(getTableName(env), spec));
        } catch (ConditionalCheckFailedException e) {
            // condition failed
            return false;
        }
        return true;
    }

    /*
     * build item from [ primary key, seedId ] pairs and expired at timestamp
     */
    private Item buildItem(Pair<PrimaryKey, String> keySeedId, long expiredAt, boolean shouldSetTTL) {
        Item item = new Item()
                .withPrimaryKey(keySeedId.getKey())
                .withString(ATTR_SEED_ID, keySeedId.getRight());
        if (shouldSetTTL) {
            item.withNumber(ATTR_EXPIRED_AT, expiredAt);
        }
        return item;
    }

    private String getSeedId(Item item) {
        if (item == null) {
            return null;
        }
        return item.getString(ATTR_SEED_ID);
    }

    /* thin wrappers */

    private long getExpiredAt() {
        return entityMatchConfigurationService.getExpiredAt();
    }

    private String getTableName(EntityMatchEnvironment environment) {
        return entityMatchConfigurationService.getTableName(environment);
    }

    private RetryTemplate getRetryTemplate(@NotNull EntityMatchEnvironment env) {
        return entityMatchConfigurationService.getRetryTemplate(env);
    }

    /*
     * helper to rebuild primary key from item
     */
    private PrimaryKey getKey(@NotNull EntityMatchEnvironment env, Item item) {
        if (item == null) {
            return null;
        }

        switch (env) {
            case STAGING:
                return new PrimaryKey(
                        ATTR_PARTITION_KEY, item.getString(ATTR_PARTITION_KEY),
                        ATTR_RANGE_KEY, item.getString(ATTR_RANGE_KEY));
            case SERVING:
                return new PrimaryKey(ATTR_PARTITION_KEY, item.getString(ATTR_PARTITION_KEY));
            default:
                throw new UnsupportedOperationException("Unsupported environment: " + env);
        }
    }

    private PrimaryKey buildKey(
            @NotNull EntityMatchEnvironment env, @NotNull Tenant tenant, @NotNull EntityLookupEntry entry, int version) {
        switch (env) {
            case STAGING:
                return buildStagingKey(tenant, entry, version);
            case SERVING:
                return buildServingKey(tenant, entry, version);
            default:
                throw new UnsupportedOperationException("Unsupported environment: " + env);
        }
    }

    /*
     * Dynamo key format:
     * - Partition Key: LOOKUP_<TENANT_PID>_<STAGING_VERSION>_<ENTITY>_<CALCULATED_SUFFIX>
     *     - E.g., "LOOKUP_123_0_Account_3"
     * - Sort Key: <LOOKUP_TYPE>_<SERIALIZED_KEY_VALUES>
     *     - E.g., "DOMAIN_COUNTRY_google.com_USA"
     */
    private PrimaryKey buildStagingKey(@NotNull Tenant tenant, @NotNull EntityLookupEntry entry, int version) {
        // use calculated suffix because we need lookup
        // & 0x7fffffff to make it positive and mod nShards
        String sortKey = serialize(entry);
        int suffix = (sortKey.hashCode() & 0x7fffffff) % numStagingShards;
        String partitionKey = String.join(DELIMITER,
                PREFIX, tenant.getId(), String.valueOf(version), entry.getEntity(), String.valueOf(suffix));
        return new PrimaryKey(ATTR_PARTITION_KEY, partitionKey, ATTR_RANGE_KEY, sortKey);
    }

    /*
     * Dynamo key format:
     * - Partition Key: SEED_<TENANT_PID>_<SERVING_VERSION>_<ENTITY>_<LOOKUP_TYPE>_<SERIALIZED_KEY_VALUES>
     *     - E.g., "LOOKUP_123_0_Account_DOMAIN_COUNTRY_google.com_USA"
     */
    private PrimaryKey buildServingKey(@NotNull Tenant tenant, @NotNull EntityLookupEntry entry, int version) {
        String lookupKey = serialize(entry);
        String partitionKey = String.join(DELIMITER,
                PREFIX, tenant.getId(), String.valueOf(version), entry.getEntity(), lookupKey);
        return new PrimaryKey(ATTR_PARTITION_KEY, partitionKey);
    }

    /*
     * Helper to generate primary key for lookup entries
     */
    private String serialize(@NotNull EntityLookupEntry entry) {
        return merge(entry.getType().name(), entry.getSerializedKeys(), entry.getSerializedValues());
    }

    private String merge(String... strs) {
        // filter out empty strings
        strs = Arrays.stream(strs).filter(StringUtils::isNotBlank).toArray(String[]::new);
        return String.join(DELIMITER, strs);
    }
}
