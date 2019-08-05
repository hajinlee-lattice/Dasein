package com.latticeengines.domain.exposed.metadata.statistics;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.util.AttributeUtils;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class AttributeRepository {

    @JsonProperty("Customer")
    private CustomerSpace customerSpace;

    @JsonProperty("DataCollection")
    private String collectionName;

    @JsonProperty("AttrMap")
    @JsonSerialize(keyUsing = AttributeLookup.AttributeLookupSerializer.class)
    @JsonDeserialize(keyUsing = AttributeLookup.AttributeLookupKeyDeserializer.class)
    private Map<AttributeLookup, ColumnMetadata> cmMap;

    @JsonProperty("TableNameMap")
    private Map<TableRoleInCollection, String> tableNameMap;

    // used for creating repository for le-query tests
    // in other cases, should always construct from data collection
    @VisibleForTesting
    public AttributeRepository(CustomerSpace customerSpace, String collectionName,
            Map<AttributeLookup, ColumnMetadata> cmMap,
            Map<TableRoleInCollection, String> tableNameMap) {
        this.collectionName = collectionName;
        this.customerSpace = customerSpace;
        this.cmMap = cmMap;
        this.tableNameMap = tableNameMap;
    }

    public AttributeRepository() {
    }

    public static AttributeRepository constructRepo(Map<String, StatsCube> statsCubes,
            Map<TableRoleInCollection, Table> tableMap, CustomerSpace customerSpace,
            String collectionName) {
        Map<TableRoleInCollection, String> tableNameMap = getTableNameMap(tableMap);
        Map<AttributeLookup, AttributeStats> statsMap = expandStats(statsCubes);
        Map<AttributeLookup, ColumnMetadata> cmMap = expandAttrs(statsMap.keySet(), tableMap);
        cmMap.forEach((lookup, cm) -> {
            AttributeStats stats = statsMap.get(lookup);
            cm.setStats(stats);
        });
        return new AttributeRepository(customerSpace, collectionName, cmMap, tableNameMap);
    }

    private static Map<TableRoleInCollection, String> getTableNameMap(
            Map<TableRoleInCollection, Table> tableMap) {
        Map<TableRoleInCollection, String> map = new HashMap<>();
        tableMap.forEach((r, t) -> map.put(r, t.getName()));
        return map;
    }

    private static Map<AttributeLookup, ColumnMetadata> expandAttrs(
            Collection<AttributeLookup> lookups, Map<TableRoleInCollection, Table> tableMap) {
        Map<TableRoleInCollection, Map<String, ColumnMetadata>> attrMaps = new HashMap<>();
        Map<AttributeLookup, ColumnMetadata> attributes = new HashMap<>();
        lookups.forEach(lookup -> {
            BusinessEntity entity = lookup.getEntity();
            TableRoleInCollection role = entity.getServingStore();
            if (tableMap.containsKey(role)) {
                Table table = tableMap.get(role);
                if (!attrMaps.containsKey(role)) {
                    Map<String, ColumnMetadata> attrMap = expandAttrsInTable(table);
                    attrMaps.put(role, attrMap);
                }
                Map<String, ColumnMetadata> attrMap = attrMaps.get(role);
                ColumnMetadata attribute = attrMap.get(lookup.getAttribute());
                if (attribute != null) {
                    attributes.put(lookup, attribute);
                }
            }
        });
        if (tableMap.containsKey(BusinessEntity.Transaction.getServingStore())) {
            Table table = tableMap.get(BusinessEntity.Transaction.getServingStore());
            Map<String, ColumnMetadata> attrMap = expandAttrsInTable(table);
            attrMap.forEach((name, md) -> attributes
                    .put(new AttributeLookup(BusinessEntity.Transaction, name), md));
        }
        return attributes;
    }

    private static Map<String, ColumnMetadata> expandAttrsInTable(Table table) {
        Map<String, ColumnMetadata> attrMap = new HashMap<>();
        table.getAttributes().forEach(attr -> {
            ColumnMetadata metadata = new ColumnMetadata();
            String javaClz = AttributeUtils.toJavaClass(attr.getPhysicalDataType(), attr.getDataType());
            if ("string".equalsIgnoreCase(javaClz)) {
                metadata.setJavaClass(javaClz);
            }
            metadata.setNumBits(attr.getNumOfBits());
            metadata.setBitOffset(attr.getBitOffset());
            metadata.setPhysicalName(attr.getPhysicalName());
            attrMap.put(attr.getName(), metadata);
        });
        return attrMap;
    }

    private static Map<AttributeLookup, AttributeStats> expandStats(
            Map<String, StatsCube> statsCubes) {
        Map<AttributeLookup, AttributeStats> statsMap = new HashMap<>();
        statsCubes.forEach((name, cube) -> {
            BusinessEntity entity = BusinessEntity.valueOf(name);
            cube.getStatistics().forEach((attrName, attrStats) -> {
                AttributeLookup attrLookup = new AttributeLookup(entity, attrName);
                statsMap.put(attrLookup, attrStats);
            });
        });
        return statsMap;
    }

    public ColumnMetadata getColumnMetadata(AttributeLookup attributeLookup) {
        return cmMap.get(attributeLookup);
    }

    public String getTableName(TableRoleInCollection tableRole) {
        return tableNameMap.get(tableRole);
    }

    public Map<TableRoleInCollection, String> getTableNameMap() {
        return tableNameMap;
    }

    public Set<String> getTableNames() {
        return new HashSet<>(tableNameMap.values());
    }

    public CustomerSpace getCustomerSpace() {
        return customerSpace;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public String getIdentifier(String sqlUser) {
        return customerSpace.getTenantId() + "-" + collectionName + "-" + sqlUser;
    }

    public boolean hasAttribute(AttributeLookup lookup) {
        return cmMap.containsKey(lookup);
    }

    public void appendServingStore(BusinessEntity entity, Table table) {
        List<AttributeLookup> toBeDeleted = cmMap.keySet().stream() //
                .filter(attrLookup -> entity.equals(attrLookup.getEntity())) //
                .collect(Collectors.toList());
        toBeDeleted.forEach(cmMap::remove);
        Map<String, ColumnMetadata> attrs = expandAttrsInTable(table);
        attrs.forEach((n, md) -> cmMap.put(new AttributeLookup(entity, n), md));
        tableNameMap.put(entity.getServingStore(), table.getName());
    }

    public void changeServingStoreTableName(TableRoleInCollection role, String tableName) {
        tableNameMap.put(role, tableName);
    }

}
