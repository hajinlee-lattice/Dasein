package com.latticeengines.datacloud.match.service.impl;

import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder;
import com.amazonaws.services.dynamodbv2.xspec.PutItemExpressionSpec;
import com.google.common.base.Preconditions;
import com.latticeengines.aws.dynamo.DynamoItemService;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.service.CDLConfigurationService;
import com.latticeengines.datacloud.match.service.CDLMatchVersionService;
import com.latticeengines.datacloud.match.service.CDLRawSeedService;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.cdl.CDLLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.cdl.CDLMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.cdl.CDLRawSeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder.N;
import static com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder.S;
import static com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder.SS;
import static com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder.attribute_not_exists;
import static com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder.if_not_exists;
import static com.latticeengines.common.exposed.util.ValidationUtils.checkNotNull;
import static com.latticeengines.domain.exposed.datacloud.match.cdl.CDLLookupEntry.Mapping.MANY_TO_MANY;
import static com.latticeengines.domain.exposed.datacloud.match.cdl.CDLLookupEntry.Mapping.MANY_TO_ONE;
import static com.latticeengines.domain.exposed.datacloud.match.cdl.CDLLookupEntry.Mapping.ONE_TO_ONE;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toSet;

@Component("cdlRawSeedService")
public class CDLRawSeedServiceImpl implements CDLRawSeedService {

    // TODO add retries

    /* constants */
    private static final String PREFIX = DataCloudConstants.CDL_PREFIX_SEED;
    private static final String PREFIX_SEED_ATTRIBUTES = DataCloudConstants.CDL_PREFIX_SEED_ATTRIBUTES;
    private static final String ATTR_PARTITION_KEY = DataCloudConstants.CDL_ATTR_PID;
    private static final String ATTR_RANGE_KEY = DataCloudConstants.CDL_ATTR_SID;
    private static final String ATTR_SEED_ID = DataCloudConstants.CDL_ATTR_SEED_ID;
    private static final String ATTR_SEED_ENTITY = DataCloudConstants.CDL_ATTR_ENTITY;
    private static final String ATTR_SEED_VERSION = DataCloudConstants.CDL_ATTR_VERSION;
    private static final String ATTR_EXPIRED_AT = DataCloudConstants.CDL_ATTR_EXPIRED_AT;
    private static final String DELIMITER = DataCloudConstants.CDL_DELIMITER;
    private static final int INITIAL_SEED_VERSION = 0;

    /* services */
    private final DynamoItemService dynamoItemService;
    private final CDLMatchVersionService cdlMatchVersionService;
    private final CDLConfigurationService cdlConfigurationService;

    private final int numStagingShards;

    @Inject
    public CDLRawSeedServiceImpl(
            DynamoItemService dynamoItemService, CDLMatchVersionService cdlMatchVersionService,
            CDLConfigurationService cdlConfigurationService) {
        this.dynamoItemService = dynamoItemService;
        this.cdlMatchVersionService = cdlMatchVersionService;
        this.cdlConfigurationService = cdlConfigurationService;
        // NOTE this will not be changed at runtime
        numStagingShards = cdlConfigurationService.getNumShards(CDLMatchEnvironment.STAGING);
    }

    @Override
    public boolean createIfNotExists(
            @NotNull CDLMatchEnvironment env, @NotNull Tenant tenant,
            @NotNull BusinessEntity entity, @NotNull String seedId) {
        checkNotNull(env, tenant, entity, seedId);
        Item item = getBaseItem(env, tenant, entity, seedId);
        return conditionalSet(getTableName(env), item);
    }

    @Override
    public boolean setIfNotExists(
            @NotNull CDLMatchEnvironment env, @NotNull Tenant tenant, @NotNull CDLRawSeed seed) {
        checkNotNull(env, tenant, seed);
        Item item = getBaseItem(env, tenant, seed.getEntity(), seed.getId());
        // set attributes
        getStringAttributes(seed).forEach(item::withString);
        getStringSetAttributes(seed).forEach(item::withStringSet);
        return conditionalSet(getTableName(env), item);
    }

    @Override
    public CDLRawSeed get(
            @NotNull CDLMatchEnvironment env, @NotNull Tenant tenant,
            @NotNull BusinessEntity entity, @NotNull String seedId) {
        checkNotNull(env, tenant, entity, seedId);

        int version = getMatchVersion(env, tenant);
        PrimaryKey key = buildKey(env, tenant, version, entity, seedId);

        Item item = dynamoItemService.getItem(getTableName(env), key);
        return fromItem(item);
    }

    @Override
    public List<CDLRawSeed> get(
            @NotNull CDLMatchEnvironment env, @NotNull Tenant tenant,
            @NotNull BusinessEntity entity, @NotNull List<String> seedIds) {
        checkNotNull(env, tenant, entity, seedIds);
        if (seedIds.isEmpty()) {
            return Collections.emptyList();
        }

        int version = getMatchVersion(env, tenant);
        List<PrimaryKey> keys = seedIds
                .stream()
                .map(id -> buildKey(env, tenant, version, entity, id))
                .collect(Collectors.toList());
        Map<String, Item> itemMap = dynamoItemService
                .batchGet(getTableName(env), keys)
                .stream()
                .filter(Objects::nonNull)
                .map(item -> Pair.of(item.getString(ATTR_SEED_ID), item))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue, (v1, v2) -> v1)); // ignore duplicates
        return seedIds.stream().map(itemMap::get).map(this::fromItem).collect(Collectors.toList());
    }

    @Override
    public CDLRawSeed updateIfNotSet(
            @NotNull CDLMatchEnvironment env, @NotNull Tenant tenant, @NotNull CDLRawSeed rawSeed) {
        checkNotNull(env, tenant, rawSeed);

        int version = getMatchVersion(env, tenant);
        PrimaryKey key = buildKey(env, tenant, version, rawSeed.getEntity(), rawSeed.getId());

        Map<String, String> strAttrs = getStringAttributes(rawSeed);
        Map<String, Set<String>> setAttrs = getStringSetAttributes(rawSeed);

        ExpressionSpecBuilder builder = new ExpressionSpecBuilder()
                .addUpdate(S(ATTR_SEED_ID).set(rawSeed.getId()))
                .addUpdate(S(ATTR_SEED_ENTITY).set(rawSeed.getEntity().name()))
                // increase the version by 1
                .addUpdate(N(ATTR_SEED_VERSION).add(1))
                // set TTL (no effect on serving)
                .addUpdate(N(ATTR_EXPIRED_AT).set(getExpiredAt()));

        strAttrs.forEach((attrName, attrValue) -> builder.addUpdate(
                S(attrName).set(if_not_exists(attrName, attrValue)))); // update if the attr not exist
        // set does not matter, just add to set
        setAttrs.forEach((attrName, attrValue) -> builder.addUpdate(SS(attrName).append(attrValue)));

        UpdateItemOutcome result = dynamoItemService.update(getTableName(env), new UpdateItemSpec()
                .withPrimaryKey(key)
                .withExpressionSpec(builder.buildForUpdate())
                .withReturnValues(ReturnValue.UPDATED_OLD));
        Preconditions.checkNotNull(result);
        Preconditions.checkNotNull(result.getUpdateItemResult());
        return fromAttributeMap(result.getUpdateItemResult().getAttributes());
    }

    @Override
    public CDLRawSeed clearIfEquals(
            @NotNull CDLMatchEnvironment env, @NotNull Tenant tenant, @NotNull CDLRawSeed rawSeed) {
        checkNotNull(env, tenant, rawSeed);

        int version = getMatchVersion(env, tenant);
        PrimaryKey key = buildKey(env, tenant, version, rawSeed.getEntity(), rawSeed.getId());

        Map<String, String> strAttrs = getStringAttributes(rawSeed);
        Map<String, Set<String>> setAttrs = getStringSetAttributes(rawSeed);

        ExpressionSpecBuilder builder = new ExpressionSpecBuilder()
                // increase the version by 1
                .addUpdate(N(ATTR_SEED_VERSION).add(1))
                .withCondition(N(ATTR_SEED_VERSION).eq(rawSeed.getVersion()))
                // set TTL (no effect on serving)
                .addUpdate(N(ATTR_EXPIRED_AT).set(getExpiredAt()));

        // clear attributes
        strAttrs.forEach((attrName, attrValue) -> builder.addUpdate(
                S(attrName).remove()));
        // remove from set
        setAttrs.forEach((attrName, attrValue) -> builder.addUpdate(SS(attrName).delete(attrValue)));

        try {
            UpdateItemOutcome result = dynamoItemService.update(getTableName(env), new UpdateItemSpec()
                    .withPrimaryKey(key)
                    .withExpressionSpec(builder.buildForUpdate())
                    // use all old because we are not updating seedID and entity
                    .withReturnValues(ReturnValue.ALL_OLD));
            Preconditions.checkNotNull(result);
            Preconditions.checkNotNull(result.getUpdateItemResult());
            return fromAttributeMap(result.getUpdateItemResult().getAttributes());
        } catch (ConditionalCheckFailedException e) {
            // someone update this seed in between
            throw new IllegalStateException(e);
        }
    }

    @Override
    public boolean delete(
            @NotNull CDLMatchEnvironment env, @NotNull Tenant tenant,
            @NotNull BusinessEntity entity, @NotNull String seedId) {
        checkNotNull(env, tenant, entity, seedId);

        int version = getMatchVersion(env, tenant);
        PrimaryKey key = buildKey(env, tenant, version, entity, seedId);
        DeleteItemOutcome result = dynamoItemService.delete(getTableName(env), new DeleteItemSpec()
                .withPrimaryKey(key)
                // return all old attributes to determine whether item is deleted
                .withReturnValues(ReturnValue.ALL_OLD));
        Preconditions.checkNotNull(result);
        Preconditions.checkNotNull(result.getDeleteItemResult());
        return MapUtils.isNotEmpty(result.getDeleteItemResult().getAttributes());
    }

    // TODO unit test this
    /*
     * Transform attribute value map to raw seed
     */
    private CDLRawSeed fromAttributeMap(Map<String, AttributeValue> map) {
        if (MapUtils.isEmpty(map)) {
            return null;
        }

        String seedId = map.get(ATTR_SEED_ID).getS();
        BusinessEntity entity = BusinessEntity.getByName(map.get(ATTR_SEED_ENTITY).getS());
        int version = map.containsKey(ATTR_SEED_VERSION)
            ? INITIAL_SEED_VERSION : Integer.parseInt(map.get(ATTR_SEED_VERSION).getN());
        List<CDLLookupEntry> entries = new ArrayList<>();
        Map<String, String> attributes = new HashMap<>();
        map.forEach((seedAttrName, value) -> {
            if (isReservedAttribute(seedAttrName)) {
                return;
            }

            // parse attribute
            String attrName = parseSeedAttrName(seedAttrName);
            if (attrName != null) {
                attributes.put(attrName, map.get(seedAttrName).getS());
                return;
            }

            // TODO log error if failed to parse lookup entries here
            entries.addAll(parseLookupEntries(entity, seedAttrName, value));
        });
        return new CDLRawSeed(seedId, entity, version, entries, attributes);
    }

    // TODO unit test this
    /*
     * Transform item to raw seed
     */
    private CDLRawSeed fromItem(Item item) {
        if (item == null) {
            return null;
        }

        String seedId = item.getString(ATTR_SEED_ID);
        BusinessEntity entity = BusinessEntity.getByName(item.getString(ATTR_SEED_ENTITY));
        int version = item.hasAttribute(ATTR_SEED_VERSION) ? item.getInt(ATTR_SEED_VERSION) : INITIAL_SEED_VERSION;
        List<CDLLookupEntry> entries = new ArrayList<>();
        Map<String, String> attributes = new HashMap<>();
        item.attributes().forEach(e -> {
            if (isReservedAttribute(e.getKey())) {
                return;
            }

            // parse attribute
            String attrName = parseSeedAttrName(e.getKey());
            if (attrName != null) {
                attributes.put(attrName, item.getString(e.getKey()));
                return;
            }

            // parse lookup key
            // TODO log error if failed to parse lookup entries here
            entries.addAll(parseLookupEntries(entity, e.getKey(), item));
        });
        return new CDLRawSeed(seedId, entity, version, entries, attributes);
    }

    /*
     * set raw seed item if the item does not already exist (has partition key)
     *
     * return true if item is set, false otherwise
     */
    private boolean conditionalSet(@NotNull String tableName, @NotNull Item item) {
        PutItemExpressionSpec expressionSpec = new ExpressionSpecBuilder()
                // use partition key to determine whether item exists
                .withCondition(attribute_not_exists(ATTR_PARTITION_KEY))
                .buildForPut();
        PutItemSpec spec = new PutItemSpec()
                .withItem(item)
                .withExpressionSpec(expressionSpec)
                .withReturnValues(ReturnValue.NONE);

        try {
            dynamoItemService.put(tableName, spec);
        } catch (ConditionalCheckFailedException e) {
            // seed already exists
            return false;
        }
        return true;
    }

    private Item getBaseItem(
            CDLMatchEnvironment env, Tenant tenant, BusinessEntity entity, String seedId) {
        int version = getMatchVersion(env, tenant);
        PrimaryKey key = buildKey(env, tenant, version, entity, seedId);
        return new Item()
                .withPrimaryKey(key)
                .withString(ATTR_SEED_ID, seedId)
                .withString(ATTR_SEED_ENTITY, entity.name())
                .withNumber(ATTR_SEED_VERSION, INITIAL_SEED_VERSION)
                // set TTL (no effect on serving)
                .withNumber(ATTR_EXPIRED_AT, getExpiredAt());
    }

    /*
     * Build all string attributes. Returns Map<attributeName, attributeValue>.
     * TODO unit test this
     */
    private Map<String, String> getStringAttributes(@NotNull CDLRawSeed seed) {
        Map<String, String> attrs = seed.getLookupEntries()
                .stream()
                .filter(entry -> entry.getType().mapping == ONE_TO_ONE || entry.getType().mapping == MANY_TO_ONE)
                .map(this::buildAttrPairFromLookupEntry)
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue, (v1, v2) -> v1));
        seed.getAttributes().forEach((attrName, value) -> {
            String seedAttrName = buildSeedAttrName(attrName);
            attrs.put(seedAttrName, value);
        });
        return attrs;
    }

    /*
     * Build all string set attributes. Returns Map<attributeName, Set<attributeValue>>.
     * TODO unit test this
     */
    private Map<String, Set<String>> getStringSetAttributes(@NotNull CDLRawSeed seed) {
        return seed.getLookupEntries()
                .stream()
                .filter(entry -> entry.getType().mapping == MANY_TO_MANY)
                .map(this::buildAttrPairFromLookupEntry)
                // group by key of pair and aggregate value as set
                .collect(groupingBy(Pair::getKey, mapping(Pair::getValue, toSet())));
    }

    /*
     * build seed attribute name/value for lookup entries
     * TODO unit test this
     */
    private Pair<String, String> buildAttrPairFromLookupEntry(@NotNull CDLLookupEntry entry) {
        String attrName = entry.getType().name();
        if (!entry.getSerializedKeys().isEmpty()) {
            attrName += DELIMITER + entry.getSerializedKeys();
        }
        return Pair.of(attrName, entry.getSerializedValues());
    }

    /*
     * Transform from dynamo attribute value to a list of lookup entries
     * (String -> single item, String Set -> multiple items)
     * TODO unit test this
     */
    private List<CDLLookupEntry> parseLookupEntries(
            @NotNull BusinessEntity entity, @NotNull String attributeName, Object value) {
        for (CDLLookupEntry.Type type : CDLLookupEntry.Type.values()) {
            int idx = attributeName.indexOf(type.name());
            if (idx < 0) {
                continue;
            }

            // NOTE attribute name = Type + DELIMITER
            // parse serialized lookup keys/values
            int startIdx = idx + type.name().length() + DELIMITER.length();
            String serializedKeys = startIdx < attributeName.length() ? attributeName.substring(startIdx) : "";
            if (value instanceof AttributeValue) {
                return getLookupEntries(type, entity, serializedKeys, (AttributeValue) value);
            } else if (value instanceof Item) {
                return getLookupEntries(type, entity, attributeName, serializedKeys, (Item) value);
            }
        }
        return Collections.emptyList();
    }

    private List<CDLLookupEntry> getLookupEntries(
            @NotNull CDLLookupEntry.Type type, @NotNull BusinessEntity entity,
            @NotNull String serializedKeys, @NotNull AttributeValue value) {
        if (value.getS() != null) {
            return Collections.singletonList(
                    new CDLLookupEntry(type, entity, serializedKeys, value.getS()));
        } else if (value.getSS() != null) {
            return value.getSS()
                    .stream()
                    .map(val -> new CDLLookupEntry(type, entity, serializedKeys, val))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    private List<CDLLookupEntry> getLookupEntries(
            @NotNull CDLLookupEntry.Type type, @NotNull BusinessEntity entity,
            @NotNull String attributeName, @NotNull String serializedKeys, @NotNull Item item) {
        Class<?> clz = item.getTypeOf(attributeName);
        if (String.class.equals(clz)) {
            return Collections.singletonList(
                    new CDLLookupEntry(type, entity, serializedKeys, item.getString(attributeName)));
        } else if (Set.class.isAssignableFrom(clz)) {
            return item.getStringSet(attributeName)
                    .stream()
                    .map(val -> new CDLLookupEntry(type, entity, serializedKeys, val))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    private long getExpiredAt() {
        return cdlConfigurationService.getExpiredAt();
    }

    /*
     * build seed attribute name for non-lookup attributes
     *
     * - original attribute name => dynamo attribute name
     */
    private String buildSeedAttrName(@NotNull String attrName) {
        return PREFIX_SEED_ATTRIBUTES + attrName;
    }

    /*
     * parse seed attribute name for non-lookup attributes
     *
     * - dynamo attribute name => original attribute name
     */
    private String parseSeedAttrName(@NotNull String seedAttrName) {
        int attrIdx = seedAttrName.indexOf(PREFIX_SEED_ATTRIBUTES);
        if (attrIdx < 0) {
            return null;
        }

        return seedAttrName.substring(attrIdx + PREFIX_SEED_ATTRIBUTES.length());
    }

    private boolean isReservedAttribute(String attrName) {
        return ATTR_SEED_ID.equals(attrName) || ATTR_SEED_ENTITY.equals(attrName)
            || ATTR_PARTITION_KEY.equals(attrName) || ATTR_RANGE_KEY.equals(attrName)
            || ATTR_SEED_VERSION.equals(attrName) || ATTR_EXPIRED_AT.equals(attrName);
    }

    private int getMatchVersion(@NotNull CDLMatchEnvironment env, @NotNull Tenant tenant) {
        return cdlMatchVersionService.getCurrentVersion(env, tenant);
    }

    private String getTableName(CDLMatchEnvironment environment) {
        return cdlConfigurationService.getTableName(environment);
    }

    private PrimaryKey buildKey(
            CDLMatchEnvironment env, Tenant tenant, int version, BusinessEntity entity, String seedId) {
        Preconditions.checkNotNull(tenant.getPid());
        switch (env) {
            case SERVING:
                return buildServingKey(tenant, version, entity, seedId);
            case STAGING:
                return buildStagingKey(tenant, version, entity, seedId);
            default:
                throw new UnsupportedOperationException("Unsupported environment: " + env);
        }
    }

    /*
     * Dynamo key format:
     * - Partition Key: SEED_<TENANT_PID>_<STAGING_VERSION>_<ENTITY>_<CALCULATED_SUFFIX>
     *     - E.g., "SEED_123_5_Account_3"
     * - Sort Key: <CDL_ACCOUNT_ID>
     *     - E.g., "aabbabc123456789"
     */
    private PrimaryKey buildStagingKey(
            Tenant tenant, int version, BusinessEntity entity, String seedId) {
        // use calculated suffix because we need lookup
        // & 0x7fffffff to make it positive and mod nShards
        int suffix = (seedId.hashCode() & 0x7fffffff) % numStagingShards;
        String partitionKey = String.join(DELIMITER,
                PREFIX, tenant.getPid().toString(), String.valueOf(version), entity.name(), String.valueOf(suffix));
        return new PrimaryKey(ATTR_PARTITION_KEY, partitionKey, ATTR_RANGE_KEY, seedId);
    }

    /*
     * Dynamo key format:
     * - Partition Key: SEED_<TENANT_PID>_<SERVING_VERSION>_<ENTITY>_<CDL_ACCOUNT_ID>
     *     - E.g., "SEED_123_5_Account_aabbabc123456789"
     */
    private PrimaryKey buildServingKey(
            Tenant tenant, int version, BusinessEntity entity, String seedId) {
        String partitionKey = String.join(DELIMITER,
                PREFIX, tenant.getPid().toString(), String.valueOf(version), entity.name(), seedId);
        return new PrimaryKey(ATTR_PARTITION_KEY, partitionKey);
    }
}
