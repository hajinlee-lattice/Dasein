package com.latticeengines.apps.cdl.service.impl;

import static com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder.N;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder;
import com.amazonaws.services.dynamodbv2.xspec.UpdateItemExpressionSpec;
import com.google.common.base.Preconditions;
import com.latticeengines.apps.cdl.service.DimensionMetadataService;
import com.latticeengines.aws.dynamo.DynamoItemService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.activity.DimensionMetadata;

@Component("dimensionMetadataService")
@Lazy
public class DimensionMetadataServiceImpl implements DimensionMetadataService {
    private static final Logger log = LoggerFactory.getLogger(DimensionMetadataServiceImpl.class);

    private static final String PREFIX = "DIM_METADATA_"; // prefix for dimension metadata
    private static final String DIM_ID_COUNTER_PREFIX = "DIM_ID_COUNTER_";
    private static final String DIM_ID_VALUE_PREFIX = "DIM_ID_VALUE_"; // dimension ID -> dimension value mapping
    private static final String DIM_VALUE_ID_PREFIX = "DIM_VALUE_ID_"; // dimesnion value -> ID inverted index
    private static final String DELIMITER = "||";
    private static final String PARTITION_KEY = "PID";
    private static final String SORT_KEY = "SID";
    private static final String ATTR_KEY = "Key";
    private static final String ATTR_VALUE = "Record";
    private static final String STREAM_ID_KEY = "StreamId";
    private static final String DIM_NAME_KEY = "DimensionName";
    private static final String DUMMY_COUNTER_SORT_KEY_VALUE = "Counter";
    private static final int RETRY_LIMIT = 5;
    // TODO add compressed metadata key if single metadata is getting too big

    private final RetryTemplate retryTemplate = RetryUtils.getRetryTemplate(RETRY_LIMIT);
    private final DynamoItemService dynamoItemService;

    @Value("${cdl.activity.store.metadata.table.name}")
    private String metadataTableName;

    @Inject
    public DimensionMetadataServiceImpl(DynamoItemService dynamoItemService) {
        this.dynamoItemService = dynamoItemService;
    }

    // TODO add ttl option for testing data later

    @Override
    public void put(@NotNull String signature, @NotNull String streamId, @NotNull String dimensionName,
            @NotNull DimensionMetadata metadata) {
        check(signature, streamId, dimensionName);
        Preconditions.checkNotNull(metadata);
        log.info("Set dimension metadata for stream {} in signature {}, metadata = {}", streamId, signature,
                JsonUtils.serialize(metadata));

        retryTemplate.execute(ctx -> {
            dynamoItemService.putItem(metadataTableName, getItem(signature, streamId, dimensionName, metadata));
            return null;
        });
    }

    @Override
    public void put(@NotNull String signature,
            @NotNull Map<String, Map<String, DimensionMetadata>> dimensionMetadataMap) {
        checkNotBlank(signature, "signature");
        Preconditions.checkNotNull(dimensionMetadataMap);

        List<Item> items = dimensionMetadataMap.entrySet().stream().flatMap(entry -> {
            checkNotBlank(entry.getKey(), "stream id");
            String streamId = entry.getKey();
            Map<String, DimensionMetadata> dimensions = entry.getValue();
            if (MapUtils.isEmpty(dimensions)) {
                return Stream.empty();
            }

            return dimensions.entrySet() //
                    .stream() //
                    .filter(Objects::nonNull) //
                    .filter(dimEntry -> StringUtils.isNotBlank(dimEntry.getKey()) && dimEntry.getValue() != null) //
                    .map(dimEntry -> getItem(signature, streamId, dimEntry.getKey(), dimEntry.getValue()));
        }).collect(Collectors.toList());

        log.info("Set dimension metadata for streams {} in signature {}, total # of metadata = {}",
                dimensionMetadataMap.keySet(), signature, items.size());

        retryTemplate.execute(ctx -> {
            dynamoItemService.batchWrite(metadataTableName, items);
            return null;
        });
    }

    @Override
    public DimensionMetadata get(@NotNull String signature, @NotNull String streamId, @NotNull String dimensionName) {
        check(signature, streamId, dimensionName);
        PrimaryKey pk = getPrimaryKey(signature, streamId, dimensionName);
        log.info("Retrieve dimension(name={}) metadata for stream {} in signature {}", dimensionName, streamId,
                signature);
        return retryTemplate.execute(ctx -> getMetadata(dynamoItemService.getItem(metadataTableName, pk)));
    }

    @Override
    public Map<String, DimensionMetadata> getMetadataInStream(@NotNull String signature, @NotNull String streamId) {
        check(signature, streamId, "dimension");
        Map<String, Item> dimensionItems = query(signature, streamId).get(streamId);
        log.info("Retrieve dimension metadata for stream {} in signature {}, # of dimensions = {}", streamId, signature,
                MapUtils.size(dimensionItems));
        return toMetadataMap(dimensionItems);
    }

    @Override
    public Map<String, Map<String, DimensionMetadata>> getMetadata(@NotNull String signature) {
        checkNotBlank(signature, "signature");
        Map<String, Map<String, DimensionMetadata>> dimensionMetadataMap = query(signature, null).entrySet() //
                .stream() //
                .map(entry -> Pair.of(entry.getKey(), toMetadataMap(entry.getValue()))) //
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        log.info("Retrieve dimension metadata in signature {}, # of streams = {}", signature,
                MapUtils.size(dimensionMetadataMap));
        return dimensionMetadataMap;
    }

    @Override
    public void delete(@NotNull String signature) {
        checkNotBlank(signature, "signature");

        AtomicInteger count = new AtomicInteger(0);
        query(signature, null).values() //
                .stream() //
                .filter(Objects::nonNull) //
                .flatMap(map -> map.values().stream()) //
                .filter(Objects::nonNull) //
                .map(item -> new PrimaryKey(PARTITION_KEY, item.getString(PARTITION_KEY), SORT_KEY,
                        item.getString(SORT_KEY))) //
                .forEach(pk -> retryTemplate.execute(ctx -> {
                    dynamoItemService.deleteItem(metadataTableName, pk);
                    count.incrementAndGet();
                    return null;
                }));
        log.info("Delete dimension metadata in signature {}, total # of metadata deleted = {}", signature, count.get());
    }

    @Override
    public Map<String, String> allocateDimensionId(@NotNull String tenantId, @NotNull Set<String> dimensionValues) {
        Preconditions.checkArgument(StringUtils.isNotBlank(tenantId), "Tenant id should not be blank");
        Preconditions.checkNotNull(dimensionValues);
        if (CollectionUtils.isEmpty(dimensionValues)) {
            return Collections.emptyMap();
        }

        Map<String, String> dimensionIds = new HashMap<>(getDimensionIds(tenantId, dimensionValues));

        List<String> notAllocateValues = dimensionValues.stream().filter(val -> !dimensionIds.containsKey(val))
                .collect(Collectors.toList());
        int nNotAllocated = notAllocateValues.size();
        int maxIdInclusive = reserveIdRange(tenantId, nNotAllocated);

        // TODO lock each mapping creation properly, currently not safe to run multiple
        // allocation at once
        List<Item> items = new ArrayList<>();
        for (int i = 0; i < notAllocateValues.size(); i++) {
            String id = String.valueOf(maxIdInclusive - i);
            String val = notAllocateValues.get(i);
            PrimaryKey idValKey = getMappingPrimaryKey(DIM_ID_VALUE_PREFIX, tenantId, id);
            PrimaryKey valIdKey = getMappingPrimaryKey(DIM_VALUE_ID_PREFIX, tenantId, val);
            Item idValItem = new Item().withPrimaryKey(idValKey).withString(ATTR_KEY, id).withString(ATTR_VALUE, val);
            Item valIdItem = new Item().withPrimaryKey(valIdKey).withString(ATTR_KEY, val).withString(ATTR_VALUE, id);
            items.add(idValItem);
            items.add(valIdItem);

            // update result
            dimensionIds.put(val, id);
        }

        retryTemplate.execute(ctx -> {
            dynamoItemService.batchWrite(metadataTableName, items);
            return null;
        });

        return dimensionIds;
    }

    @Override
    public Map<String, String> getDimensionValues(@NotNull String tenantId, @NotNull Set<String> dimensionIds) {
        Preconditions.checkArgument(StringUtils.isNotBlank(tenantId), "Tenant id should not be blank");
        Preconditions.checkNotNull(dimensionIds);
        return getMapping(tenantId, DIM_ID_VALUE_PREFIX, dimensionIds);
    }

    @Override
    public Map<String, String> getDimensionIds(@NotNull String tenantId, @NotNull Set<String> dimensionValues) {
        Preconditions.checkArgument(StringUtils.isNotBlank(tenantId), "Tenant id should not be blank");
        Preconditions.checkNotNull(dimensionValues);
        return getMapping(tenantId, DIM_VALUE_ID_PREFIX, dimensionValues);
    }

    // reserve $count IDs, return the max reserved ID, inclusive
    // e.g., count=3 and return 6 means reserved IDs are 4,5,6
    private int reserveIdRange(@NotNull String tenantId, int count) {
        // use dummy sort key
        PrimaryKey counterKey = getMappingPrimaryKey(DIM_ID_COUNTER_PREFIX, tenantId, DUMMY_COUNTER_SORT_KEY_VALUE);
        // incr by reserve count
        UpdateItemExpressionSpec updateSpec = new ExpressionSpecBuilder() //
                .addUpdate(N(ATTR_VALUE).add(count)) //
                .buildForUpdate();
        UpdateItemSpec spec = new UpdateItemSpec().withPrimaryKey(counterKey) //
                .withExpressionSpec(updateSpec) //
                .withReturnValues(ReturnValue.UPDATED_NEW); // get the updated id (max reserved)
        UpdateItemOutcome result = dynamoItemService.update(metadataTableName, spec);
        Preconditions.checkNotNull(result);
        Preconditions.checkNotNull(result.getItem());
        return result.getItem().getInt(ATTR_VALUE);
    }

    private Map<String, String> getMapping(@NotNull String tenantId, @NotNull String prefix,
            @NotNull Set<String> keys) {
        List<String> keyList = new ArrayList<>(keys);
        List<PrimaryKey> primaryKeys = keyList.stream() //
                .map(key -> getMappingPrimaryKey(prefix, tenantId, key)) //
                .collect(Collectors.toList());
        List<Item> items = dynamoItemService.batchGet(metadataTableName, primaryKeys);
        return items.stream() //
                .filter(Objects::nonNull) //
                .filter(item -> item.hasAttribute(ATTR_KEY) && item.hasAttribute(ATTR_VALUE)) //
                .collect(Collectors.toMap(item -> item.getString(ATTR_KEY), item -> item.getString(ATTR_VALUE)));
    }

    // streamIds == null means query all in signature
    private Map<String, Map<String, Item>> query(String signature, String streamId) {
        Map<String, Map<String, Item>> dimensionItems = new HashMap<>();
        List<Item> items = retryTemplate
                .execute(ctx -> dynamoItemService.query(metadataTableName, querySpec(signature, streamId)));
        if (CollectionUtils.isNotEmpty(items)) {
            items.forEach(item -> {
                String id = item.getString(STREAM_ID_KEY);
                String dimName = item.getString(DIM_NAME_KEY);
                dimensionItems.putIfAbsent(id, new HashMap<>());
                dimensionItems.get(id).put(dimName, item);
            });
            log.info("Query dimension metadata with signature {} and stream {}, result size = {}", signature, streamId,
                    items.size());
        }
        return dimensionItems;
    }

    private QuerySpec querySpec(String signature, String streamId) {
        QuerySpec spec = new QuerySpec() //
                .withHashKey(PARTITION_KEY, PREFIX + signature);
        if (StringUtils.isNotBlank(streamId)) {
            spec = spec.withRangeKeyCondition(new RangeKeyCondition(SORT_KEY).beginsWith(streamId));
        }
        return spec;
    }

    private Item getItem(String signature, String streamId, String dimensionName, DimensionMetadata metadata) {
        return new Item().withPrimaryKey(getPrimaryKey(signature, streamId, dimensionName)) //
                .withString(STREAM_ID_KEY, streamId) //
                .withString(DIM_NAME_KEY, dimensionName) //
                .withString(ATTR_VALUE, JsonUtils.serialize(metadata));
    }

    private Map<String, DimensionMetadata> toMetadataMap(Map<String, Item> items) {
        if (MapUtils.isEmpty(items)) {
            return Collections.emptyMap();
        }
        return items.entrySet() //
                .stream() //
                .map(entry -> Pair.of(entry.getKey(), getMetadata(entry.getValue()))) //
                .filter(pair -> pair.getValue() != null) //
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

    private DimensionMetadata getMetadata(Item item) {
        if (item == null) {
            return null;
        }
        Preconditions.checkArgument(item.hasAttribute(ATTR_VALUE),
                "Missing dimension metadata value attribute " + ATTR_VALUE);
        return JsonUtils.deserialize(item.getString(ATTR_VALUE), DimensionMetadata.class);
    }

    /*-
     * either dimId -> dimValue or inverted index
     */
    private PrimaryKey getMappingPrimaryKey(@NotNull String prefix, @NotNull String tenantId, @NotNull String sortKey) {
        return new PrimaryKey(PARTITION_KEY, prefix + tenantId, SORT_KEY, sortKey);
    }

    /*-
     * pk: prefix + signature
     * sk: streamId||dimensionName
     *
     * 1. search all dimension in stream => sk.startsWith(streamId)
     * 2. search all dimensions => query pk
     */
    private PrimaryKey getPrimaryKey(String signature, String streamId, String dimensionName) {
        String sortKey = streamId + DELIMITER + dimensionName;
        return new PrimaryKey(PARTITION_KEY, PREFIX + signature, SORT_KEY, sortKey);
    }

    private void check(String signature, String streamId, String dimensionName) {
        checkNotBlank(signature, streamId);
        checkNotBlank(dimensionName, "dimension name");
    }

    private void checkNotBlank(String str, String name) {
        Preconditions.checkArgument(StringUtils.isNotBlank(str), String.format("%s should not be blank", name));
    }
}
