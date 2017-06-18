package com.latticeengines.domain.exposed.metadata.statistics;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class AttributeRepository {

    private CustomerSpace customerSpace;
    private String collectionName;

    private Map<AttributeLookup, Attribute> attributeMap;
    private Map<TableRoleInCollection, String> tableNameMap;

    // used for creating repository for le-query tests
    // in other cases, should always construct from data collection
    @VisibleForTesting
    public AttributeRepository(CustomerSpace customerSpace, String collectionName,
            Map<AttributeLookup, Attribute> attributeMap, Map<TableRoleInCollection, String> tableNameMap) {
        this.collectionName = collectionName;
        this.customerSpace = customerSpace;
        this.attributeMap = attributeMap;
        this.tableNameMap = tableNameMap;
    }

    public Attribute getAttribute(AttributeLookup attributeLookup) {
        return attributeMap.get(attributeLookup);
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
        Map<AttributeLookup, Attribute> attrMap = expandAttrs(statsMap.keySet(), tableMap);
        attrMap.forEach((lookup, attr) -> {
            AttributeStats stats = statsMap.get(lookup);
            attr.setStats(stats);
        });
        return new AttributeRepository(customerSpace, collectionName, attrMap, tableNameMap);
    }

    private static Map<TableRoleInCollection, String> getTableNameMap(Map<TableRoleInCollection, Table> tableMap) {
        Map<TableRoleInCollection, String> map = new HashMap<>();
        tableMap.forEach((r, t) -> map.put(r, t.getName()));
        return map;
    }

    private static Map<AttributeLookup, Attribute> expandAttrs(Collection<AttributeLookup> lookups,
            Map<TableRoleInCollection, Table> tableMap) {
        Map<TableRoleInCollection, Map<String, Attribute>> attrMaps = new HashMap<>();
        Map<AttributeLookup, Attribute> attributes = new HashMap<>();
        lookups.forEach(lookup -> {
            BusinessEntity entity = lookup.getEntity();
            TableRoleInCollection role = entity.getServingStore();
            Table table = tableMap.get(role);
            if (!attrMaps.containsKey(role)) {
                Map<String, Attribute> attrMap = expandAttrsInTable(table);
                attrMaps.put(role, attrMap);
            }
            Map<String, Attribute> attrMap = attrMaps.get(role);
            Attribute attribute = attrMap.get(lookup.getAttribute());
            attributes.put(lookup, attribute);
        });
        return attributes;
    }

    private static Map<String, Attribute> expandAttrsInTable(Table table) {
        Map<String, Attribute> attrMap = new HashMap<>();
        table.getAttributes().forEach(attr -> {
            attrMap.put(attr.getName(), attr);
        });
        return attrMap;
    }

    private static Map<AttributeLookup, AttributeStats> expandStats(Statistics statistics) {
        Map<AttributeLookup, AttributeStats> statsMap = new HashMap<>();
        statistics.getCategories().values().forEach(cat -> {
            cat.getSubcategories().values().forEach(subcat -> {
                statsMap.putAll(subcat.getAttributes());
            });
        });
        return statsMap;
    }

}
