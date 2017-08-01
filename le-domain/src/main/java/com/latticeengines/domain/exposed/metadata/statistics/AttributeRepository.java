package com.latticeengines.domain.exposed.metadata.statistics;

import java.util.Collection;
import java.util.HashMap;
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
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;

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
            Map<AttributeLookup, ColumnMetadata> cmMap, Map<TableRoleInCollection, String> tableNameMap) {
        this.collectionName = collectionName;
        this.customerSpace = customerSpace;
        this.cmMap = cmMap;
        this.tableNameMap = tableNameMap;
    }

    public AttributeRepository() {
    }

    public ColumnMetadata getColumnMetadata(AttributeLookup attributeLookup) {
        return cmMap.get(attributeLookup);
    }

    public String getTableName(TableRoleInCollection tableRole) {
        return tableNameMap.get(tableRole);
    }

    public CustomerSpace getCustomerSpace() {
        return customerSpace;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public String getIdentifier() {
        return customerSpace.getTenantId() + "-" + collectionName;
    }

    public static List<TableRoleInCollection> extractServingRoles(Statistics statistics) {
        Set<BusinessEntity> entitySet = statistics.getCategories().values().stream()
                .flatMap(cat -> cat.getSubcategories().values().stream()
                        .flatMap(subcat -> subcat.getAttributes().keySet().stream().map(AttributeLookup::getEntity)))
                .collect(Collectors.toSet());
        return entitySet.stream().map(BusinessEntity::getServingStore).collect(Collectors.toList());
    }

    public static AttributeRepository constructRepo(Statistics statistics, Map<TableRoleInCollection, Table> tableMap,
            CustomerSpace customerSpace, String collectionName) {
        Map<TableRoleInCollection, String> tableNameMap = getTableNameMap(tableMap);
        Map<AttributeLookup, AttributeStats> statsMap = expandStats(statistics);
        Map<AttributeLookup, ColumnMetadata> cmMap = expandAttrs(statsMap.keySet(), tableMap);
        return constructRepo(statistics, tableNameMap, cmMap, customerSpace, collectionName);
    }

    public static AttributeRepository constructRepo(Statistics statistics, Map<TableRoleInCollection, String> tableNameMap,
                                                    Map<AttributeLookup, ColumnMetadata> cmMap,
                                                    CustomerSpace customerSpace, String collectionName) {
        Map<AttributeLookup, AttributeStats> statsMap = expandStats(statistics);
        cmMap.forEach((lookup, cm) -> {
            AttributeStats stats = statsMap.get(lookup);
            cm.setStats(stats);
        });
        return new AttributeRepository(customerSpace, collectionName, cmMap, tableNameMap);
    }

    private static Map<TableRoleInCollection, String> getTableNameMap(Map<TableRoleInCollection, Table> tableMap) {
        Map<TableRoleInCollection, String> map = new HashMap<>();
        tableMap.forEach((r, t) -> map.put(r, t.getName()));
        return map;
    }

    private static Map<AttributeLookup, ColumnMetadata> expandAttrs(Collection<AttributeLookup> lookups,
            Map<TableRoleInCollection, Table> tableMap) {
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
                if (attribute == null) {
                    throw new RuntimeException("Cannot find metadata for attribute " + lookup);
                }
                //TODO: should set category in metadata table
                if (BusinessEntity.Account.getServingStore().equals(role)) {
                    attribute.setCategory(Category.ACCOUNT_ATTRIBUTES);
                }
                attributes.put(lookup, attribute);
            }
        });
        return attributes;
    }

    private static Map<String, ColumnMetadata> expandAttrsInTable(Table table) {
        Map<String, ColumnMetadata> attrMap = new HashMap<>();
        table.getAttributes().forEach(attr -> attrMap.put(attr.getName(), attr.getColumnMetadata()));
        return attrMap;
    }

    private static Map<AttributeLookup, AttributeStats> expandStats(Statistics statistics) {
        Map<AttributeLookup, AttributeStats> statsMap = new HashMap<>();
        statistics.getCategories().values().forEach(cat -> //
                cat.getSubcategories().values().forEach(subcat -> {
            statsMap.putAll(subcat.getAttributes());
        }));
        return statsMap;
    }

}
