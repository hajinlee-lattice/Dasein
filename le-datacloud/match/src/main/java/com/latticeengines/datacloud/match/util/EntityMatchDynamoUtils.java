package com.latticeengines.datacloud.match.util;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.security.Tenant;

/**
 * Dynamodb specific utility functions for entity match
 */
public final class EntityMatchDynamoUtils {

    protected EntityMatchDynamoUtils() {
        throw new UnsupportedOperationException();
    }

    /* constants */
    private static final String LOOKUP_PREFIX = DataCloudConstants.ENTITY_PREFIX_LOOKUP;
    private static final String ATTR_PARTITION_KEY = DataCloudConstants.ENTITY_ATTR_PID;
    private static final String ATTR_RANGE_KEY = DataCloudConstants.ENTITY_ATTR_SID;
    private static final String ATTR_SEED_ID = DataCloudConstants.ENTITY_ATTR_SEED_ID;
    private static final String DELIMITER = DataCloudConstants.ENTITY_DELIMITER;

    public static String getSeedId(Map<String, AttributeValue> attrs) {
        if (MapUtils.isEmpty(attrs) || !attrs.containsKey(ATTR_SEED_ID)) {
            return null;
        }

        return attrs.get(ATTR_SEED_ID).getS();
    }

    /*
     * wrapper to transform value map to attrValue map
     */
    public static Map<String, AttributeValue> attributeValues(Map<String, Object> valueMap) {
        if (valueMap == null) {
            return null;
        }

        return valueMap.entrySet().stream() //
                .filter(Objects::nonNull) //
                .filter(entry -> entry.getValue() != null) //
                .map(entry -> {
                    Object val = entry.getValue();
                    if (val instanceof AttributeValue) {
                        return Pair.of(entry.getKey(), (AttributeValue) val);
                    } else if (val instanceof Set) {
                        @SuppressWarnings("unchecked")
                        Set<String> set = (Set<String>) val;
                        AttributeValue attrVal = new AttributeValue();
                        attrVal.withSS(set);
                        return Pair.of(entry.getKey(), attrVal);
                    } else if (val instanceof Number) {
                        AttributeValue attrVal = new AttributeValue();
                        attrVal.withN(val.toString());
                        return Pair.of(entry.getKey(), attrVal);
                    } else {
                        return Pair.of(entry.getKey(), new AttributeValue(val.toString()));
                    }
                }) //
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

    }

    /*
     * wrapper to transform primary key to corresponding map representation
     */
    public static Map<String, AttributeValue> primaryKey(@NotNull PrimaryKey primaryKey) {
        Preconditions.checkNotNull(primaryKey);

        return primaryKey.getComponents() //
                .stream() //
                .filter(Objects::nonNull) //
                .filter(comp -> comp.getValue() != null) //
                .collect(Collectors.toMap(KeyAttribute::getName,
                        attr -> new AttributeValue(attr.getValue().toString())));
    }

    /*
     * build primary key for lookup entry
     */
    public static PrimaryKey buildLookupPKey(@NotNull EntityMatchEnvironment env, @NotNull Tenant tenant,
            @NotNull EntityLookupEntry entry, int version, int nShards) {
        switch (env) {
        case STAGING:
            return buildStagingKey(tenant, entry, version, nShards);
        case SERVING:
            return buildServingKey(tenant, entry, version);
        default:
            throw new UnsupportedOperationException("Unsupported environment: " + env);
        }
    }

    /*
     * Dynamo key format: - Partition Key:
     * LOOKUP_<TENANT_PID>_<STAGING_VERSION>_<ENTITY>_<CALCULATED_SUFFIX> - E.g.,
     * "LOOKUP_123_0_Account_3" - Sort Key: <LOOKUP_TYPE>_<SERIALIZED_KEY_VALUES> -
     * E.g., "DOMAIN_COUNTRY_google.com_USA"
     */
    private static PrimaryKey buildStagingKey(@NotNull Tenant tenant, @NotNull EntityLookupEntry entry, int version,
            int nShards) {
        // use calculated suffix because we need lookup
        // & 0x7fffffff to make it positive and mod nShards
        String sortKey = serialize(entry);
        int suffix = (sortKey.hashCode() & 0x7fffffff) % nShards;
        String partitionKey = String.join(DELIMITER, LOOKUP_PREFIX, tenant.getId(), String.valueOf(version),
                entry.getEntity(), String.valueOf(suffix));
        return new PrimaryKey(ATTR_PARTITION_KEY, partitionKey, ATTR_RANGE_KEY, sortKey);
    }

    /*
     * Dynamo key format: - Partition Key:
     * SEED_<TENANT_PID>_<SERVING_VERSION>_<ENTITY>_<LOOKUP_TYPE>_<
     * SERIALIZED_KEY_VALUES> - E.g.,
     * "LOOKUP_123_0_Account_DOMAIN_COUNTRY_google.com_USA"
     */
    private static PrimaryKey buildServingKey(@NotNull Tenant tenant, @NotNull EntityLookupEntry entry, int version) {
        String lookupKey = serialize(entry);
        String partitionKey = String.join(DELIMITER, LOOKUP_PREFIX, tenant.getId(), String.valueOf(version),
                entry.getEntity(), lookupKey);
        return new PrimaryKey(ATTR_PARTITION_KEY, partitionKey);
    }

    /*
     * Helper to generate primary key for lookup entries
     */
    private static String serialize(@NotNull EntityLookupEntry entry) {
        return merge(entry.getType().name(), entry.getSerializedKeys(), entry.getSerializedValues());
    }

    private static String merge(String... strs) {
        // filter out empty strings
        strs = Arrays.stream(strs).filter(StringUtils::isNotBlank).toArray(String[]::new);
        return String.join(DELIMITER, strs);
    }
}
