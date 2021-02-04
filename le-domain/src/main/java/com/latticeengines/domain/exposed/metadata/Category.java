package com.latticeengines.domain.exposed.metadata;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.collect.Sets;
import com.latticeengines.domain.exposed.util.ActivityStoreUtils;

public enum Category {
    RATING("Lattice Ratings", 0), //
    FIRMOGRAPHICS("Firmographics", 1), //
    ACCOUNT_ATTRIBUTES("My Attributes", 2), //
    CURATED_ACCOUNT_ATTRIBUTES("Curated Account Attributes", 3), //
    CONTACT_ATTRIBUTES("Contact Attributes", 4), //
    CURATED_CONTACT_ATTRIBUTES("Curated Contact Attributes", 5), //
    PRODUCT_SPEND("Product Spend Profile", 6), //
    INTENT("Intent", 7), //
    WEBSITE_PROFILE("Website Profile", 8), //
    TECHNOLOGY_PROFILE("HG Technology Profile", 9), //
    ONLINE_PRESENCE("Online Presence", 10), //
    GROWTH_TRENDS("Growth Trends", 11), //
    WEBSITE_KEYWORDS("Website Keywords", 12), //
    ACCOUNT_INFORMATION("Account Information", 13), //
    LEAD_INFORMATION("Lead Information", 14), //
    DEFAULT("Default", 15), //
    AI_INSIGHTS("AI Insights", 16), //
    WEB_VISIT_PROFILE("My Website Visits", 17) {
        @Override
        public FilterOptions getFilterOptions() {
//            return ActivityStoreUtils.attrFilterOptions();
            return ActivityStoreUtils.genericFilterOptions();
        }

        @Override
        public boolean shouldShowSubCategoryInCategoryTile() {
            return true;
        }

        @Override
        public String getSecondaryDisplayName() {
            return ActivityStoreUtils.defaultTimeFilterDisplayName();
        }
    }, //
    OPPORTUNITY_PROFILE("My Opportunities", 18), //
    ACCOUNT_MARKETING_ACTIVITY_PROFILE("My Account Marketing Activity", 19) {
        @Override
        public FilterOptions getFilterOptions() {
//            return ActivityStoreUtils.attrFilterOptions();
            return ActivityStoreUtils.genericFilterOptions();
        }

        @Override
        public boolean shouldShowSubCategoryInCategoryTile() {
            return true;
        }

        @Override
        public String getSecondaryDisplayName() {
            return ActivityStoreUtils.defaultTimeFilterDisplayName();
        }
    }, //
    CONTACT_MARKETING_ACTIVITY_PROFILE("My Contact Marketing Activity", 20) {
        @Override
        public FilterOptions getFilterOptions() {
//            return ActivityStoreUtils.attrFilterOptions();
            return ActivityStoreUtils.genericFilterOptions();
        }

        @Override
        public boolean shouldShowSubCategoryInCategoryTile() {
            return true;
        }

        @Override
        public String getSecondaryDisplayName() {
            return ActivityStoreUtils.defaultTimeFilterDisplayName();
        }
    },
    DNBINTENTDATA_PROFILE("D&B Intent", 21), //
    COVID_19("COVID-19", 22), //
    DNB_TECHNOLOGY_PROFILE("D&B Technology Profile", 23); //

    public static final String SUB_CAT_OTHER = "Other";
    public static final String SUB_CAT_ACCOUNT_IDS = "System Account IDs";
    public static final String SUB_CAT_CONTACT_IDS = "System Contact IDs";

    private static Map<String, Category> nameMap;
    private static Set<String> values;
    private static List<Category> premiumCategories = Arrays.asList(INTENT, DNB_TECHNOLOGY_PROFILE, TECHNOLOGY_PROFILE,
            WEBSITE_KEYWORDS, ACCOUNT_ATTRIBUTES, CONTACT_ATTRIBUTES, GROWTH_TRENDS, COVID_19);
    // used in following scenarios
    // 1. iteration metadata API called via remodeling UI
    // 2. attribute management UI
    private static Set<Category> hiddenFromUiCategories = new HashSet<>(
            Arrays.asList(LEAD_INFORMATION, DEFAULT, ACCOUNT_INFORMATION));
    private static Set<Category> ldcReservedCategories = Sets.newHashSet(FIRMOGRAPHICS, //
            GROWTH_TRENDS, //
            COVID_19, //
            INTENT, //
            ONLINE_PRESENCE, //
            DNB_TECHNOLOGY_PROFILE, //
            TECHNOLOGY_PROFILE, //
            WEBSITE_KEYWORDS, //
            WEBSITE_PROFILE, //
            AI_INSIGHTS //
    );

    static {
        nameMap = new HashMap<>();
        for (Category category : Category.values()) {
            nameMap.put(category.getName(), category);
        }
        values = new HashSet<>(
                Arrays.stream(values()).map(Category::name).collect(Collectors.toSet()));
    }

    private final String name;
    private final int order;

    Category(String name, int order) {
        this.name = name;
        this.order = order;
    }

    public static Category fromName(String name) {
        if (StringUtils.isBlank(name)) {
            return null;
        }
        if (values.contains(name)) {
            return valueOf(name);
        } else if (nameMap.containsKey(name)) {
            return nameMap.get(name);
        } else {
            String categoryName = name.replaceAll(" ", "_").toUpperCase();
            if (values.contains(categoryName)) {
                return valueOf(categoryName);
            } else {
                categoryName = StringEscapeUtils.unescapeHtml4(name);
                if (nameMap.containsKey(categoryName)) {
                    return nameMap.get(categoryName);
                }
                throw new IllegalArgumentException("Cannot find a Category with name " + name);
            }
        }
    }

    public static List<Category> getPremiumCategories() {
        return premiumCategories;
    }

    public static Set<Category> getLdcReservedCategories() {
        return ldcReservedCategories;
    }

    public String getName() {
        return this.name;
    }

    public Integer getOrder() {
        return this.order;
    }

    @Override
    public String toString() {
        return this.name;
    }

    public boolean isHiddenFromUi() {
        return hiddenFromUiCategories.contains(this);
    }

    public boolean isPremium() {
        return premiumCategories.contains(this);
    }

    public boolean isLdcReservedCategory() {
        return ldcReservedCategories.contains(this);
    }

    // attribute filter drop down options for attributes in this category
    public FilterOptions getFilterOptions() {
        // default no attribute filtering
        return null;
    }

    // label shown under category name in category tile
    public String getSecondaryDisplayName() {
        // default no secondary display name
        return null;
    }

    // flag to show sub category (instead of attribute name) in category
    public boolean shouldShowSubCategoryInCategoryTile() {
        return false;
    }

    public static class CategoryKeyDeserializer extends KeyDeserializer {
        public CategoryKeyDeserializer() {
        }

        @Override
        public Object deserializeKey(String key, DeserializationContext ctxt) throws IOException {
            return Category.fromName(key);
        }
    }

    public static class CategoryKeySerializer extends JsonSerializer<Category> {
        public CategoryKeySerializer() {
        }

        @Override
        public void serialize(Category value, JsonGenerator jgen, SerializerProvider provider)
                throws IOException {
            jgen.writeFieldName(value.getName());
        }
    }

    public static class CategoryOrderComparator implements Comparator<Category> {
        @Override
        public int compare(Category c1, Category c2) {
            return c1.order - c2.order;
        }
    }
}
