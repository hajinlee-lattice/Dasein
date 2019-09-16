package com.latticeengines.datacloud.match.service.impl;

import static com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder.N;
import static com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder.S;
import static com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder.SS;
import static com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder.attribute_not_exists;
import static com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder.if_not_exists;
import static com.latticeengines.common.exposed.util.ValidationUtils.checkNotNull;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry.Mapping.MANY_TO_MANY;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry.Mapping.MANY_TO_ONE;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry.Mapping.ONE_TO_ONE;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder;
import com.amazonaws.services.dynamodbv2.xspec.PutItemExpressionSpec;
import com.amazonaws.services.dynamodbv2.xspec.UpdateAction;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.latticeengines.aws.dynamo.DynamoItemService;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.service.EntityMatchConfigurationService;
import com.latticeengines.datacloud.match.service.EntityRawSeedService;
import com.latticeengines.datacloud.match.util.EntityMatchUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;
import com.latticeengines.domain.exposed.security.Tenant;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@Component("entityRawSeedService")
public class EntityRawSeedServiceImpl implements EntityRawSeedService {
    private static final Logger log = LoggerFactory.getLogger(EntityRawSeedServiceImpl.class);

    /* constants */
    private static final String PREFIX = DataCloudConstants.ENTITY_PREFIX_SEED;
    private static final String PREFIX_SEED_ATTRIBUTES = DataCloudConstants.ENTITY_PREFIX_SEED_ATTRIBUTES;
    private static final String ATTR_PARTITION_KEY = DataCloudConstants.ENTITY_ATTR_PID;
    private static final String ATTR_RANGE_KEY = DataCloudConstants.ENTITY_ATTR_SID;
    private static final String ATTR_SEED_ID = DataCloudConstants.ENTITY_ATTR_SEED_ID;
    private static final String ATTR_SEED_ENTITY = DataCloudConstants.ENTITY_ATTR_ENTITY;
    private static final String ATTR_SEED_VERSION = DataCloudConstants.ENTITY_ATTR_VERSION;
    private static final String ATTR_EXPIRED_AT = DataCloudConstants.ENTITY_ATTR_EXPIRED_AT;
    private static final String DELIMITER = DataCloudConstants.ENTITY_DELIMITER;
    private static final int INITIAL_SEED_VERSION = 0;

    /* services */
    private final DynamoItemService dynamoItemService;
    private final EntityMatchConfigurationService entityMatchConfigurationService;

    private static final Scheduler scheduler = Schedulers.newParallel("entity-rawseed");

    @Inject
    public EntityRawSeedServiceImpl(
            DynamoItemService dynamoItemService, EntityMatchConfigurationService entityMatchConfigurationService) {
        this.dynamoItemService = dynamoItemService;
        this.entityMatchConfigurationService = entityMatchConfigurationService;
    }

    @Override
    public boolean createIfNotExists(
            @NotNull EntityMatchEnvironment env, @NotNull Tenant tenant,
            @NotNull String entity, @NotNull String seedId, boolean setTTL, int version) {
        checkNotNull(env, tenant, entity, seedId);
        Item item = getBaseItem(env, tenant, entity, seedId, setTTL, version);
        return getRetryTemplate(env).execute(ctx ->
                conditionalSet(getTableName(env), item));
    }

    @Override
    public boolean setIfNotExists(
            @NotNull EntityMatchEnvironment env, @NotNull Tenant tenant, @NotNull EntityRawSeed seed, boolean setTTL,
            int version) {
        checkNotNull(env, tenant, seed);
        Item item = getItemFromSeed(env, tenant, seed, setTTL, version);
        return getRetryTemplate(env).execute(ctx ->
                conditionalSet(getTableName(env), item));
    }

    @Override
    public EntityRawSeed get(
            @NotNull EntityMatchEnvironment env, @NotNull Tenant tenant,
            @NotNull String entity, @NotNull String seedId, int version) {
        checkNotNull(env, tenant, entity, seedId);

        PrimaryKey key = buildKey(env, tenant, version, entity, seedId);

        Item item = getRetryTemplate(env).execute(ctx ->
                dynamoItemService.getItem(getTableName(env), key));
        return fromItem(item);
    }

    @Override
    public List<EntityRawSeed> get(
            @NotNull EntityMatchEnvironment env, @NotNull Tenant tenant,
            @NotNull String entity, @NotNull List<String> seedIds, int version) {
        checkNotNull(env, tenant, entity, seedIds);
        if (seedIds.isEmpty()) {
            return Collections.emptyList();
        }

        List<PrimaryKey> keys = seedIds
                .stream()
                .map(id -> buildKey(env, tenant, version, entity, id))
                .distinct()
                .collect(Collectors.toList());
        Map<String, Item> itemMap = getRetryTemplate(env).execute(ctx -> {
            return dynamoItemService
                    .batchGet(getTableName(env), keys)
                    .stream()
                    .filter(Objects::nonNull)
                    .map(item -> Pair.of(item.getString(ATTR_SEED_ID), item))
                    .collect(Collectors.toMap(Pair::getKey, Pair::getValue, (v1, v2) -> v1)); // ignore duplicates
        });
        return seedIds.stream().map(itemMap::get).map(this::fromItem).collect(Collectors.toList());
    }

    @Override
    public Map<Integer, List<EntityRawSeed>> scan(
            @NotNull EntityMatchEnvironment env, @NotNull Tenant tenant,
            @NotNull String entity, List<String> seedIds, int maxResultSize, int version) {
        checkNotNull(env, tenant, entity);
        if (!EntityMatchEnvironment.STAGING.equals(env)) {
            throw new UnsupportedOperationException(String.format("Scanning for %s is not supported.", env.name()));
        }
        Map<Integer, String> seedMap = new HashMap<>();
        int numStagingShards = numStagingShards();
        if (CollectionUtils.isEmpty(seedIds)) {
            for (int i = 0; i < numStagingShards; i++) {
                seedMap.put(i, "");
            }
        } else {
            seedIds.forEach(seedId -> {
                seedMap.put(getShardId(seedId), seedId);
            });
        }
        List<Pair<Integer, Item>> itemPairs = scanPartition(env, tenant, entity, seedMap, maxResultSize, version) //
                .sequential() //
                .collectList() //
                .block();
        Map<Integer, List<EntityRawSeed>> result = new HashMap<>();
        if (CollectionUtils.isNotEmpty(itemPairs)) {
            itemPairs.forEach(itemPair -> {
                result.putIfAbsent(itemPair.getLeft(), new ArrayList<>());
                result.get(itemPair.getLeft()).add(fromItem(itemPair.getRight()));
            });
            return result;
        } else {
            return null;
        }
    }

    private ParallelFlux<Pair<Integer, Item>> scanPartition(EntityMatchEnvironment env, Tenant tenant, String entity,
            Map<Integer, String> seedMap, int maxResultSize, int version) {
        Integer[] shardIds = new Integer[seedMap.keySet().size()];
        shardIds = seedMap.keySet().toArray(shardIds);
        return Flux.just(shardIds).parallel().runOn(scheduler)
                .map(k -> {
                    PrimaryKey primaryKey = StringUtils.isEmpty(seedMap.get(k)) ? null : buildKey(env, tenant,
                                    version, entity, seedMap.get(k));
                    QuerySpec querySpec = new QuerySpec() //
                            .withKeyConditionExpression(ATTR_PARTITION_KEY + " = :v_pk")
                            .withValueMap(new ValueMap().withString(":v_pk",
                                    getShardPartitionKey(tenant, version, entity, k)))
                            .withExclusiveStartKey(primaryKey) //
                            .withMaxResultSize(maxResultSize);
                    List<Pair<Integer, Item>> result = new ArrayList<>();
                    dynamoItemService.query(getTableName(env), querySpec)
                            .stream()
                            .filter(Objects::nonNull)
                            .forEach(item -> {
                                result.add(new ImmutablePair<>(k, item));
                            });
                    return result;
                }).flatMap(Flux::fromIterable);
    }

    @Override
    public EntityRawSeed updateIfNotSet(
            @NotNull EntityMatchEnvironment env, @NotNull Tenant tenant,
            @NotNull EntityRawSeed rawSeed, boolean setTTL, int version) {
        checkNotNull(env, tenant, rawSeed);

        PrimaryKey key = getPrimaryKey(env, tenant, rawSeed, version);
        ExpressionSpecBuilder builder = new ExpressionSpecBuilder()
                .addUpdate(S(ATTR_SEED_ID).set(rawSeed.getId()))
                .addUpdate(S(ATTR_SEED_ENTITY).set(rawSeed.getEntity()))
                // increase the version by 1
                .addUpdate(N(ATTR_SEED_VERSION).add(1));

        if (setTTL) {
            builder.addUpdate(N(ATTR_EXPIRED_AT).set(getExpiredAt()));
        }

        getStringAttributes(rawSeed) //
                .forEach((attrName, attrValue) -> builder
                        .addUpdate(getStringAttrUpdateAction(rawSeed.getEntity(), attrName, attrValue)));
        // set does not matter, just add to set
        getStringSetAttributes(rawSeed)
                .forEach((attrName, attrValue) -> builder.addUpdate(SS(attrName).append(attrValue)));

        // TODO There is a trade-off on getting back the entire old item or only the updated attributes.
        //      decide whether we want more detailed report or safe bandwidth
        UpdateItemOutcome result = getRetryTemplate(env).execute(ctx ->
                dynamoItemService.update(getTableName(env),
                        new UpdateItemSpec()
                                .withPrimaryKey(key)
                                .withExpressionSpec(builder.buildForUpdate())
                                .withReturnValues(ReturnValue.UPDATED_OLD)));
        Preconditions.checkNotNull(result);
        Preconditions.checkNotNull(result.getUpdateItemResult());
        return fromAttributeMap(result.getUpdateItemResult().getAttributes());
    }

    @Override
    public EntityRawSeed clearIfEquals(
            @NotNull EntityMatchEnvironment env, @NotNull Tenant tenant, @NotNull EntityRawSeed rawSeed, int version) {
        return clear(env, tenant, rawSeed, version, true);
    }

    @Override
    public EntityRawSeed clear(EntityMatchEnvironment env, Tenant tenant, EntityRawSeed rawSeed, int version) {
        return clear(env, tenant, rawSeed, version, false);
    }

    @Override
    public boolean delete(
            @NotNull EntityMatchEnvironment env, @NotNull Tenant tenant,
            @NotNull String entity, @NotNull String seedId, int version) {
        checkNotNull(env, tenant, entity, seedId);

        PrimaryKey key = buildKey(env, tenant, version, entity, seedId);
        return getRetryTemplate(env).execute(ctx -> dynamoItemService.deleteItem(getTableName(env), key));
    }

    @Override
    public boolean batchCreate(
            @NotNull EntityMatchEnvironment env, @NotNull Tenant tenant, List<EntityRawSeed> rawSeeds, boolean setTTL,
            int version) {
        checkNotNull(env, tenant);
        if (CollectionUtils.isEmpty(rawSeeds)) {
            return false;
        }
        List<Item> batchItems = rawSeeds.stream() //
                .map(seed -> getItemFromSeed(env, tenant, seed, setTTL, version)) //
                .collect(Collectors.toList());
        dynamoItemService.batchWrite(getTableName(env), batchItems);
        return true;
    }

    /*
     * 1. clear all lookup entries & attributes that are in the input seed.
     * 2. if useOptimisticLocking, will only clear if the existing seed has the same internal version as the one
     * specified in input seed.
     */
    private EntityRawSeed clear(
            @NotNull EntityMatchEnvironment env, @NotNull Tenant tenant,
            @NotNull EntityRawSeed rawSeed, Integer version, boolean useOptimisticLocking) {
        checkNotNull(env, tenant, rawSeed);

        PrimaryKey key = getPrimaryKey(env, tenant, rawSeed, version);
        // not setting ttl to honor the original setting
        ExpressionSpecBuilder builder = new ExpressionSpecBuilder();

        if (useOptimisticLocking) {
            // increase the version by 1
            builder.addUpdate(N(ATTR_SEED_VERSION).add(1))
                    .withCondition(N(ATTR_SEED_VERSION).eq(rawSeed.getVersion()));
        }

        // clear attributes
        getStringAttributes(rawSeed).forEach((attrName, attrValue) -> builder.addUpdate(
                S(attrName).remove()));
        // remove from set
        getStringSetAttributes(rawSeed)
                .forEach((attrName, attrValue) -> builder.addUpdate(SS(attrName).delete(attrValue)));

        try {
            UpdateItemOutcome result = getRetryTemplate(env).execute(ctx ->
                    dynamoItemService.update(getTableName(env),
                            new UpdateItemSpec()
                                    .withPrimaryKey(key)
                                    .withExpressionSpec(builder.buildForUpdate())
                                    // use all old because we are not updating seedID and entity
                                    .withReturnValues(ReturnValue.ALL_OLD)));
            Preconditions.checkNotNull(result);
            Preconditions.checkNotNull(result.getUpdateItemResult());
            return fromAttributeMap(result.getUpdateItemResult().getAttributes());
        } catch (ConditionalCheckFailedException e) {
            // someone update this seed in between
            throw new IllegalStateException(e);
        }
    }

    /*
     * Transform attribute value map to raw seed
     */
    @VisibleForTesting
    protected EntityRawSeed fromAttributeMap(Map<String, AttributeValue> map) {
        if (MapUtils.isEmpty(map)) {
            return null;
        }

        String seedId = map.get(ATTR_SEED_ID).getS();
        String entity = map.get(ATTR_SEED_ENTITY).getS();
        int version = map.containsKey(ATTR_SEED_VERSION)
            ? Integer.parseInt(map.get(ATTR_SEED_VERSION).getN()) : INITIAL_SEED_VERSION;
        List<EntityLookupEntry> entries = new ArrayList<>();
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

            List<EntityLookupEntry> lookupEntries = parseLookupEntries(entity, seedAttrName, value);
            if (CollectionUtils.isNotEmpty(lookupEntries)) {
                entries.addAll(lookupEntries);
            } else {
                log.error("Failed to parse lookup entries. Seed ID = {}, entity = {}, attrName = {}, attrValue = {}",
                        seedId, entity, seedAttrName, value);
            }
        });
        return new EntityRawSeed(seedId, entity, version, entries, attributes);
    }

    /*
     * Transform item to raw seed
     */
    @VisibleForTesting
    protected EntityRawSeed fromItem(Item item) {
        if (item == null) {
            return null;
        }

        String seedId = item.getString(ATTR_SEED_ID);
        String entity = item.getString(ATTR_SEED_ENTITY);
        int version = item.hasAttribute(ATTR_SEED_VERSION) ? item.getInt(ATTR_SEED_VERSION) : INITIAL_SEED_VERSION;
        List<EntityLookupEntry> entries = new ArrayList<>();
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
            List<EntityLookupEntry> lookupEntries = parseLookupEntries(entity, e.getKey(), item);
            if (CollectionUtils.isNotEmpty(lookupEntries)) {
                entries.addAll(lookupEntries);
            } else {
                log.error("Failed to parse lookup entries in item." +
                        " Seed ID = {}, entity = {}, attrName = {}, attrValue = {}",
                        seedId, entity, e.getKey(), e.getValue());
            }
        });
        return new EntityRawSeed(seedId, entity, version, entries, attributes);
    }

    /**
     * Generate dynamo update action for input string attribute
     *
     * @param entity
     *            target entity of current match
     * @param attrName
     *            attribute name to update
     * @param attrValue
     *            attribute value to update
     * @return generated dynamo update action, will not be {@literal null}
     */
    private UpdateAction getStringAttrUpdateAction(@NotNull String entity, @NotNull String attrName, String attrValue) {
        if (!attrName.startsWith(PREFIX_SEED_ATTRIBUTES)
                || !EntityMatchUtils.shouldOverrideAttribute(entity, attrName)) {
            Preconditions.checkNotNull(attrValue, String.format("Cannot update attribute %s to null value", attrName));
            // lookup keys or first win attributes
            return S(attrName).set(if_not_exists(attrName, attrValue));
        }

        // last win attributes
        if (attrValue == null) {
            // remove attribute when value is null
            return S(attrName).remove();
        } else {
            // override
            return S(attrName).set(attrValue);
        }
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
            EntityMatchEnvironment env, Tenant tenant, String entity, String seedId, boolean shouldSetTTL,
            int version) {
        PrimaryKey key = buildKey(env, tenant, version, entity, seedId);
        Item item =  new Item()
                .withPrimaryKey(key)
                .withString(ATTR_SEED_ID, seedId)
                .withString(ATTR_SEED_ENTITY, entity)
                .withNumber(ATTR_SEED_VERSION, INITIAL_SEED_VERSION);
        if (shouldSetTTL) {
            item.withNumber(ATTR_EXPIRED_AT, getExpiredAt());
        }
        return item;
    }

    private Item getItemFromSeed(EntityMatchEnvironment env, Tenant tenant, EntityRawSeed seed, boolean setTTL,
            int version) {
        Item item = getBaseItem(env, tenant, seed.getEntity(), seed.getId(), setTTL, version);
        // set attributes
        getStringAttributes(seed).forEach(item::withString);
        getStringSetAttributes(seed).forEach(item::withStringSet);
        return item;
    }

    private PrimaryKey getPrimaryKey(
            @NotNull EntityMatchEnvironment env, @NotNull Tenant tenant, @NotNull EntityRawSeed rawSeed,
            Integer version) {
        return buildKey(env, tenant, version, rawSeed.getEntity(), rawSeed.getId());
    }

    /*
     * Build all string attributes. Returns Map<attributeName, attributeValue>.
     */
    @VisibleForTesting
    protected Map<String, String> getStringAttributes(@NotNull EntityRawSeed seed) {
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
     */
    @VisibleForTesting
    protected Map<String, Set<String>> getStringSetAttributes(@NotNull EntityRawSeed seed) {
        return seed.getLookupEntries()
                .stream()
                .filter(entry -> entry.getType().mapping == MANY_TO_MANY)
                .map(this::buildAttrPairFromLookupEntry)
                // group by key of pair and aggregate value as set
                .collect(groupingBy(Pair::getKey, mapping(Pair::getValue, toSet())));
    }

    /*
     * build seed attribute name/value for lookup entries
     */
    @VisibleForTesting
    protected Pair<String, String> buildAttrPairFromLookupEntry(@NotNull EntityLookupEntry entry) {
        String attrName = entry.getType().name();
        if (!entry.getSerializedKeys().isEmpty()) {
            attrName += DELIMITER + entry.getSerializedKeys();
        }
        return Pair.of(attrName, entry.getSerializedValues());
    }

    /*
     * Transform from dynamo attribute value to a list of lookup entries
     * (String -> single item, String Set -> multiple items)
     */
    private List<EntityLookupEntry> parseLookupEntries(
            @NotNull String entity, @NotNull String attributeName, Object value) {
        for (EntityLookupEntry.Type type : EntityLookupEntry.Type.values()) {
            int idx = attributeName.indexOf(type.name());
            if (idx != 0) {
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

    private List<EntityLookupEntry> getLookupEntries(
            @NotNull EntityLookupEntry.Type type, @NotNull String entity,
            @NotNull String serializedKeys, @NotNull AttributeValue value) {
        if (value.getS() != null) {
            return Collections.singletonList(
                    new EntityLookupEntry(type, entity, serializedKeys, value.getS()));
        } else if (value.getSS() != null) {
            return value.getSS()
                    .stream()
                    .map(val -> new EntityLookupEntry(type, entity, serializedKeys, val))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    private List<EntityLookupEntry> getLookupEntries(
            @NotNull EntityLookupEntry.Type type, @NotNull String entity,
            @NotNull String attributeName, @NotNull String serializedKeys, @NotNull Item item) {
        Class<?> clz = item.getTypeOf(attributeName);
        if (String.class.equals(clz)) {
            return Collections.singletonList(
                    new EntityLookupEntry(type, entity, serializedKeys, item.getString(attributeName)));
        } else if (Set.class.isAssignableFrom(clz)) {
            return item.getStringSet(attributeName)
                    .stream()
                    .map(val -> new EntityLookupEntry(type, entity, serializedKeys, val))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    private RetryTemplate getRetryTemplate(@NotNull EntityMatchEnvironment env) {
        return entityMatchConfigurationService.getRetryTemplate(env);
    }

    private long getExpiredAt() {
        return entityMatchConfigurationService.getExpiredAt();
    }

    /*
     * build seed attribute name for non-lookup attributes
     *
     * - original attribute name => dynamo attribute name
     */
    @VisibleForTesting
    protected String buildSeedAttrName(@NotNull String attrName) {
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

    private String getTableName(EntityMatchEnvironment environment) {
        return entityMatchConfigurationService.getTableName(environment);
    }

    private PrimaryKey buildKey(
            EntityMatchEnvironment env, Tenant tenant, int version, String entity, String seedId) {
        Preconditions.checkNotNull(tenant.getId());
        String partitionKey = getPartitionKey(env, tenant, version, entity, seedId);
        switch (env) {
            case SERVING:
                return buildServingKey(partitionKey);
            case STAGING:
                return buildStagingKey(partitionKey, seedId);
            default:
                throw new UnsupportedOperationException("Unsupported environment: " + env);
        }
    }

    /*
     * Dynamo key format:
     * - Partition Key: SEED_<TENANT_ID>_<STAGING_VERSION>_<ENTITY>_<CALCULATED_SUFFIX>
     *     - E.g., "SEED_123_5_Account_3"
     * - Sort Key: <ENTITY_ID>
     *     - E.g., "aabbabc123456789"
     */
    private PrimaryKey buildStagingKey(String partitionKey, String seedId) {
        return new PrimaryKey(ATTR_PARTITION_KEY, partitionKey, ATTR_RANGE_KEY, seedId);
    }

    /*
     * Dynamo key format:
     * - Partition Key: SEED_<TENANT_ID>_<SERVING_VERSION>_<ENTITY>_<ENTITY_ID>
     *     - E.g., "SEED_123_5_Account_aabbabc123456789"
     */
    private PrimaryKey buildServingKey(String partitionKey) {
        return new PrimaryKey(ATTR_PARTITION_KEY, partitionKey);
    }

    private String getPartitionKey(EntityMatchEnvironment env, Tenant tenant, int version, String entity, String seedId) {
        switch (env) {
            case STAGING:
                int shardsId = getShardId(seedId);
                return getShardPartitionKey(tenant, version, entity, shardsId);
            case SERVING:
                return String.join(DELIMITER,
                        PREFIX, tenant.getId(), String.valueOf(version), entity, seedId);
            default:
                throw new UnsupportedOperationException("Unsupported environment: " + env);
        }
    }

    private int numStagingShards() {
        return entityMatchConfigurationService.getNumShards(EntityMatchEnvironment.STAGING);
    }

    private int getShardId(String seedId) {
        // use calculated suffix because we need lookup
        // & 0x7fffffff to make it positive and mod nShards
        return (seedId.hashCode() & 0x7fffffff) % numStagingShards();
    }

    private String getShardPartitionKey(Tenant tenant, int version, String entity, int shardId) {
        return String.join(DELIMITER,
                PREFIX, tenant.getId(), String.valueOf(version), entity, String.valueOf(shardId));
    }
}
