package com.latticeengines.domain.exposed.query;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask.SubType;

public enum EntityType {
    Accounts(BusinessEntity.Account, null, "Accounts"), //
    Contacts(BusinessEntity.Contact, null, "Contacts"), //
    ProductPurchases(BusinessEntity.Transaction, null, "Product Purchases"), //
    ProductBundles(BusinessEntity.Product, SubType.Bundle, "Product Bundles"), //
    ProductHierarchy(BusinessEntity.Product, SubType.Hierarchy, "Product Hierarchy");
    private BusinessEntity entity;
    private SubType subType;
    private String displayName;

    private static List<String> names;
    static {
        names = new ArrayList<String>();
        for (EntityType entry : values()) {
            names.add(entry.getDisplayName());
        }
    }

    EntityType(BusinessEntity entity, SubType subType, String displayName) {
        this.entity = entity;
        this.subType = subType;
        this.displayName = displayName;
    }

    public static List<String> getNames() {
        return names;
    }

    public static EntityType fromEntityAndSubType(BusinessEntity entity, SubType subType) {
        for (EntityType entry : values()) {
            if (entry.getEntity().equals(entity) && entry.getSubType() == subType) {
                return entry;
            }
        }
        if (subType == null) {
            throw new IllegalArgumentException(
                    String.format("There is no entity %s with null type in EntityType", entity.name()));
        } else {
            throw new IllegalArgumentException(
                    String.format("There is no entity %s or type %s in EntityType", entity.name(), subType.name()));
        }
    }

    public static EntityType fromDisplayNameToEntityType(String displayName) {
        for (EntityType entry : values()) {
            if (entry.getDisplayName().equals(displayName)) {
                return entry;
            }
        }
        throw new IllegalArgumentException(String.format("There is no corresponding EntityType for %s", displayName));
    }

    public static EntityType fromFeedTypeName(String feedTypeName) {//which feedtype is feedtype without systemName
        for (EntityType entry : values()) {
            if (entry.getDefaultFeedTypeName().equalsIgnoreCase(feedTypeName)) {
                return entry;
            }
        }
        return null;
    }

    public BusinessEntity getEntity() {
        return entity;
    }

    public SubType getSubType() {
        return subType;
    }

    public String getDisplayName() {
        return displayName;
    }

    public String getDefaultFeedTypeName() {
        return this.subType == null ? entity.name() + "Data" : entity.name() + subType.name();
    }

    public static List<String> getDefaultFolders() {
        Set<String> defaultFolders = new HashSet<>();
        for (EntityType entry : values()) {
            defaultFolders.add(entry.getDefaultFeedTypeName());
        }
        return new ArrayList<>(defaultFolders);
    }

}
