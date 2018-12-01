package com.latticeengines.datacloud.match.service.impl;

import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder;
import com.amazonaws.services.dynamodbv2.xspec.PutItemExpressionSpec;
import com.google.common.base.Preconditions;
import com.latticeengines.aws.dynamo.DynamoItemService;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.service.CDLConfigurationService;
import com.latticeengines.datacloud.match.service.CDLLookupEntryService;
import com.latticeengines.datacloud.match.service.CDLMatchVersionService;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.cdl.CDLLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.cdl.CDLMatchEnvironment;
import com.latticeengines.domain.exposed.security.Tenant;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder.S;
import static com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder.attribute_not_exists;
import static com.latticeengines.common.exposed.util.ValidationUtils.checkNotNull;

@Component("cdlLookupEntryService")
public class CDLLookupEntryServiceImpl implements CDLLookupEntryService {

    /*
     * TODO add retries
     */
    /* constants */
    private static final String PREFIX = DataCloudConstants.CDL_PREFIX_LOOKUP;
    private static final String ATTR_PARTITION_KEY = DataCloudConstants.CDL_ATTR_PID;
    private static final String ATTR_RANGE_KEY = DataCloudConstants.CDL_ATTR_SID;
    private static final String ATTR_SEED_ID = DataCloudConstants.CDL_ATTR_SEED_ID;
    private static final String ATTR_EXPIRED_AT = DataCloudConstants.CDL_ATTR_EXPIRED_AT;
    private static final String DELIMITER = DataCloudConstants.CDL_DELIMITER;

    /* services */
    private final DynamoItemService dynamoItemService;
    private final CDLMatchVersionService cdlMatchVersionService;
    private final CDLConfigurationService cdlConfigurationService;
    private final int numStagingShards;

    @Inject
    public CDLLookupEntryServiceImpl(
            DynamoItemService dynamoItemService, CDLMatchVersionService cdlMatchVersionService,
            CDLConfigurationService cdlConfigurationService) {
        this.dynamoItemService = dynamoItemService;
        this.cdlMatchVersionService = cdlMatchVersionService;
        this.cdlConfigurationService = cdlConfigurationService;
        // NOTE this will not be changed at runtime
        numStagingShards = cdlConfigurationService.getNumShards(CDLMatchEnvironment.STAGING);
    }

    @Override
    public String get(
            @NotNull CDLMatchEnvironment env, @NotNull Tenant tenant, @NotNull CDLLookupEntry lookupEntry) {
        checkNotNull(env, tenant, lookupEntry);
        int version = getMatchVersion(env, tenant);
        PrimaryKey key = buildKey(env, tenant, lookupEntry, version);
        String tableName = getTableName(env);
        return getSeedId(dynamoItemService.getItem(tableName, key));
    }

    @Override
    public List<String> get(
            @NotNull CDLMatchEnvironment env, @NotNull Tenant tenant, @NotNull List<CDLLookupEntry> lookupEntries) {
        checkNotNull(env, tenant, lookupEntries);
        if (lookupEntries.isEmpty()) {
            return Collections.emptyList();
        }
        int version = getMatchVersion(env, tenant);
        List<PrimaryKey> keys = lookupEntries
                .stream()
                .map(entry -> buildKey(env, tenant, entry, version))
                .collect(Collectors.toList());
        // dedup, batchGet does not allow duplicate entries
        Set<PrimaryKey> uniqueKeys = new HashSet<>(keys);
        Map<PrimaryKey, Item> itemMap = dynamoItemService
                .batchGet(getTableName(env), new ArrayList<>(uniqueKeys))
                .stream()
                .filter(Objects::nonNull)
                .map(item -> Pair.of(getKey(env, item), item))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue, (v1, v2) -> v1)); // ignore duplicates
        return keys.stream().map(itemMap::get).map(this::getSeedId).collect(Collectors.toList());
    }

    @Override
    public boolean createIfNotExists(
            @NotNull CDLMatchEnvironment env, @NotNull Tenant tenant,
            @NotNull CDLLookupEntry lookupEntry, @NotNull String seedId) {
        checkNotNull(env, tenant, lookupEntry, seedId);
        PutItemExpressionSpec expressionSpec = new ExpressionSpecBuilder()
                // use partition key to determine whether item exists
                .withCondition(attribute_not_exists(ATTR_PARTITION_KEY))
                .buildForPut();
        return conditionalSet(env, tenant, lookupEntry, seedId, expressionSpec);
    }

    @Override
    public boolean setIfEquals(
            @NotNull CDLMatchEnvironment env, @NotNull Tenant tenant,
            @NotNull CDLLookupEntry lookupEntry, @NotNull String seedId) {
        checkNotNull(env, tenant, lookupEntry, seedId);
        PutItemExpressionSpec expressionSpec = new ExpressionSpecBuilder()
                // use partition key to determine whether item exists
                .withCondition(attribute_not_exists(ATTR_SEED_ID).or(S(ATTR_SEED_ID).eq(seedId)))
                .buildForPut();
        return conditionalSet(env, tenant, lookupEntry, seedId, expressionSpec);
    }

    @Override
    public void set(
            @NotNull CDLMatchEnvironment env, @NotNull Tenant tenant,
            @NotNull List<Pair<CDLLookupEntry, String>> pairs) {
        checkNotNull(env, tenant, pairs);
        if (pairs.isEmpty()) {
            return;
        }

        int version = getMatchVersion(env, tenant);
        long expiredAt = getExpiredAt();
        Map<PrimaryKey, String> seedIdMap = pairs
                .stream()
                .map(pair -> Pair.of(buildKey(env, tenant, pair.getKey(), version), pair.getValue()))
                .collect(Collectors.toMap(Pair::getLeft, Pair::getRight, (v1, v2) -> v1));
        List<Item> items = seedIdMap
                .entrySet()
                .stream()
                .map(entry -> buildItem(Pair.of(entry.getKey(), entry.getValue()), expiredAt))
                .collect(Collectors.toList());

        // batch set
        dynamoItemService.batchWrite(getTableName(env), items);
    }

    @Override
    public boolean delete(
            @NotNull CDLMatchEnvironment env, @NotNull Tenant tenant, @NotNull CDLLookupEntry lookupEntry) {
        checkNotNull(env, tenant, lookupEntry);
        int version = getMatchVersion(env, tenant);
        PrimaryKey key = buildKey(env, tenant, lookupEntry, version);
        DeleteItemOutcome result = dynamoItemService.delete(getTableName(env), new DeleteItemSpec()
                .withPrimaryKey(key)
                // return all old attributes to determine whether item is deleted
                .withReturnValues(ReturnValue.ALL_OLD));
        Preconditions.checkNotNull(result);
        Preconditions.checkNotNull(result.getDeleteItemResult());
        return MapUtils.isNotEmpty(result.getDeleteItemResult().getAttributes());
    }

    /*
     * Update lookup entry with condition specified in the given expression spec
     */
    private boolean conditionalSet(
            @NotNull CDLMatchEnvironment env, @NotNull Tenant tenant, @NotNull CDLLookupEntry lookupEntry,
            @NotNull String seedId, @NotNull PutItemExpressionSpec expressionSpec) {
        int version = getMatchVersion(env, tenant);
        PrimaryKey key = buildKey(env, tenant, lookupEntry, version);
        Item item = new Item()
                .withPrimaryKey(key)
                .withString(ATTR_SEED_ID, seedId)
                // set TTL (no effect on serving)
                .withNumber(ATTR_EXPIRED_AT, getExpiredAt());
        PutItemSpec spec = new PutItemSpec()
                .withItem(item)
                .withExpressionSpec(expressionSpec)
                .withReturnValues(ReturnValue.NONE);

        try {
            dynamoItemService.put(getTableName(env), spec);
        } catch (ConditionalCheckFailedException e) {
            // condition failed
            return false;
        }
        return true;
    }

    /*
     * build item from [ primary key, seedId ] pairs and expired at timestamp
     */
    private Item buildItem(Pair<PrimaryKey, String> keySeedId, long expiredAt) {
        return new Item()
                .withPrimaryKey(keySeedId.getKey())
                .withString(ATTR_SEED_ID, keySeedId.getRight())
                .withNumber(ATTR_EXPIRED_AT, expiredAt);
    }

    private String getSeedId(Item item) {
        if (item == null) {
            return null;
        }
        return item.getString(ATTR_SEED_ID);
    }

    /* thin wrappers */

    private long getExpiredAt() {
        return cdlConfigurationService.getExpiredAt();
    }

    private int getMatchVersion(@NotNull CDLMatchEnvironment env, @NotNull Tenant tenant) {
        return cdlMatchVersionService.getCurrentVersion(env, tenant);
    }

    private String getTableName(CDLMatchEnvironment environment) {
        return cdlConfigurationService.getTableName(environment);
    }

    /*
     * helper to rebuild primary key from item
     */
    private PrimaryKey getKey(@NotNull CDLMatchEnvironment env, Item item) {
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
            @NotNull CDLMatchEnvironment env, @NotNull Tenant tenant, @NotNull CDLLookupEntry entry, int version) {
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
    private PrimaryKey buildStagingKey(@NotNull Tenant tenant, @NotNull CDLLookupEntry entry, int version) {
        // use calculated suffix because we need lookup
        // & 0x7fffffff to make it positive and mod nShards
        String sortKey = serialize(entry);
        int suffix = (sortKey.hashCode() & 0x7fffffff) % numStagingShards;
        String partitionKey = String.join(DELIMITER,
                PREFIX, tenant.getPid().toString(), String.valueOf(version),
                entry.getEntity().name(), String.valueOf(suffix));
        return new PrimaryKey(ATTR_PARTITION_KEY, partitionKey, ATTR_RANGE_KEY, sortKey);
    }

    /*
     * Dynamo key format:
     * - Partition Key: SEED_<TENANT_PID>_<SERVING_VERSION>_<ENTITY>_<LOOKUP_TYPE>_<SERIALIZED_KEY_VALUES>
     *     - E.g., "LOOKUP_123_0_Account_DOMAIN_COUNTRY_google.com_USA"
     */
    private PrimaryKey buildServingKey(@NotNull Tenant tenant, @NotNull CDLLookupEntry entry, int version) {
        String lookupKey = serialize(entry);
        String partitionKey = String.join(DELIMITER,
                PREFIX, tenant.getPid().toString(), String.valueOf(version), entry.getEntity().name(), lookupKey);
        return new PrimaryKey(ATTR_PARTITION_KEY, partitionKey);
    }

    /*
     * Helper to generate primary key for lookup entries
     */
    private String serialize(@NotNull CDLLookupEntry entry) {
        return merge(entry.getType().name(), entry.getSerializedKeys(), entry.getSerializedValues());
    }

    private String merge(String... strs) {
        // filter out empty strings
        strs = Arrays.stream(strs).filter(StringUtils::isNotBlank).toArray(String[]::new);
        return String.join(DELIMITER, strs);
    }
}
