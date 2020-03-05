package com.latticeengines.domain.exposed.query;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask.SubType;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;

public enum EntityType {
    Accounts(BusinessEntity.Account, null, "Accounts", "AccountData", SchemaInterpretation.Account), //
    Contacts(BusinessEntity.Contact, null, "Contacts", "ContactData", SchemaInterpretation.Contact), //
    Leads(BusinessEntity.Contact, SubType.Lead, "Leads", "LeadsData", SchemaInterpretation.Contact),
    ProductPurchases(BusinessEntity.Transaction, null, "Product Purchases", "TransactionData", SchemaInterpretation.Transaction), //
    ProductBundles(BusinessEntity.Product, SubType.Bundle, "Product Bundles", "ProductBundle", SchemaInterpretation.Product), //
    ProductHierarchy(BusinessEntity.Product, SubType.Hierarchy, "Product Hierarchy", "ProductHierarchy", SchemaInterpretation.Product),
    WebVisit(BusinessEntity.ActivityStream, null, "Web Visit", "WebVisitData", SchemaInterpretation.WebVisit),
    WebVisitPathPattern(BusinessEntity.Catalog, null, "Website Path", "WebVisitPathPattern",
            SchemaInterpretation.WebVisitPathPattern), //
    WebVisitSourceMedium(BusinessEntity.Catalog, SubType.SourceMedium, "Visit Source", "WebVisitSourceMedium",
            SchemaInterpretation.WebVisitSourceMedium),
    Opportunity(BusinessEntity.ActivityStream, null, "Opportunity", "OpportunityData", SchemaInterpretation.Opportunity),
    OpportunityStageName(BusinessEntity.Catalog, SubType.StageName, "Opportunity Stage", "OpportunityStageName",
            SchemaInterpretation.OpportunityStageName);

    private static final Set<EntityType> STANDARD_ENTITY_TYPES =
            ImmutableSet.of(Accounts, Contacts, Leads, ProductPurchases, ProductBundles, ProductHierarchy);
    private static final Set<EntityType> STREAM_ENTITY_TYPES =
            ImmutableSet.of(WebVisit, WebVisitPathPattern, WebVisitSourceMedium);


    private BusinessEntity entity;
    private SubType subType;
    private String displayName;
    private String feedType;
    private SchemaInterpretation schemaInterpretation;

    private static List<String> names;
    static {
        names = new ArrayList<String>();
        for (EntityType entry : values()) {
            names.add(entry.getDisplayName());
        }
    }

    EntityType(BusinessEntity entity, SubType subType, String displayName, String feedType, SchemaInterpretation schemaInterpretation) {
        this.entity = entity;
        this.subType = subType;
        this.displayName = displayName;
        this.feedType = feedType;
        this.schemaInterpretation = schemaInterpretation;
    }

    public static List<String> getNames() {
        return names;
    }

    public static EntityType fromDataFeedTask(DataFeedTask task) {
        if (task == null) {
            throw new IllegalArgumentException("Cannot get EntityType from NULL dataFeedTask!");
        }
        EntityType entityType = fromFeedTypeName(task.getFeedType());
        if (entityType != null) {
            return entityType;
        }
        return fromEntityAndSubType(BusinessEntity.getByName(task.getEntity()), task.getSubType());
    }

    private static EntityType fromEntityAndSubType(BusinessEntity entity, SubType subType) {
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
        return feedType;
    }

    public SchemaInterpretation getSchemaInterpretation() {
        return schemaInterpretation;
    }

    public static List<String> getDefaultFolders() {
        Set<String> defaultFolders = new HashSet<>();
        for (EntityType entry : values()) {
            defaultFolders.add(entry.getDefaultFeedTypeName());
        }
        return new ArrayList<>(defaultFolders);
    }

    public static boolean isStandardEntityType(EntityType entityType) {
        return STANDARD_ENTITY_TYPES.contains(entityType);
    }

    public static boolean isStreamEntityType(EntityType entityType) {
        return STREAM_ENTITY_TYPES.contains(entityType);
    }

}
