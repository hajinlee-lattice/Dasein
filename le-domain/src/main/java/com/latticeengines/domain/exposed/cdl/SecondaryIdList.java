package com.latticeengines.domain.exposed.cdl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.collections4.map.HashedMap;
import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.query.EntityType;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class SecondaryIdList {

    @JsonProperty("secondary_id_items")
    private Map<EntityType, SecondaryIdItem> secondaryIdItems = new HashedMap<>();

    @JsonIgnore
    public void addSecondaryId(EntityType entityType, String secondaryId) {
        Preconditions.checkNotNull(entityType);
        if (secondaryIdItems.containsKey(entityType)) {
            if (!secondaryIdItems.get(entityType).getColumnName().equals(secondaryId)) {
                throw new RuntimeException(String.format("EntityType %s already have secondary Id: %s",
                        entityType.name(), secondaryId));
            }
        }

        SecondaryIdItem idItem = new SecondaryIdItem();
        idItem.setEntityType(entityType);
        idItem.setColumnName(secondaryId);
        idItem.setPriority(secondaryIdItems.size() + 1);
        secondaryIdItems.put(entityType, idItem);
    }

    @JsonIgnore
    public String getSecondaryId(EntityType entityType) {
        Preconditions.checkNotNull(entityType);
        if (MapUtils.isEmpty(secondaryIdItems)) {
            return StringUtils.EMPTY;
        }
        SecondaryIdItem secondaryIdItem = secondaryIdItems.get(entityType);
        return secondaryIdItem == null ? StringUtils.EMPTY : secondaryIdItem.getColumnName();
    }

    @JsonIgnore
    public List<String> getSecondaryIds(SortBy sortBy) {
        if (MapUtils.isEmpty(secondaryIdItems)) {
            return Collections.emptyList();
        }
        Comparator<SecondaryIdItem> comparator;
        switch (sortBy) {
            case NONE:
                comparator = null;
                break;
            case ENTITY_TYPE:
                comparator = Comparator.comparing(SecondaryIdItem::getEntityType);
                break;
            default:
                comparator = Comparator.comparing(SecondaryIdItem::getPriority);
                break;
        }
        List<SecondaryIdItem> sortedIds = new ArrayList<>(secondaryIdItems.values());
        if (comparator != null) {
            sortedIds.sort(comparator);
        }
        return sortedIds.stream().map(SecondaryIdItem::getColumnName).collect(Collectors.toList());
    }

    public enum SortBy {
        PRIORITY, ENTITY_TYPE, NONE
    }

    public static class SecondaryIdItem {

        @NotNull
        @JsonProperty("entity_type")
        private EntityType entityType;

        @JsonProperty("priority")
        private int priority;

        @JsonProperty("column_name")
        private String columnName;

        public EntityType getEntityType() {
            return entityType;
        }

        public void setEntityType(EntityType entityType) {
            this.entityType = entityType;
        }

        public int getPriority() {
            return priority;
        }

        public void setPriority(int priority) {
            this.priority = priority;
        }

        public String getColumnName() {
            return columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }
    }
}
