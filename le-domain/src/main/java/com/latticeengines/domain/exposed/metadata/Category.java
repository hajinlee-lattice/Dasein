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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.collect.Sets;

public enum Category {
    RATING("Lattice Ratings", 0), //
    FIRMOGRAPHICS("Firmographics", 1), //
    ACCOUNT_ATTRIBUTES("My Attributes", 2), //
    CURATED_ACCOUNT_ATTRIBUTES("Curated Account Attributes", 3), //
    CONTACT_ATTRIBUTES("Contact Attributes", 4), //
    PRODUCT_SPEND("Product Spend Profile", 5), //
    INTENT("Intent", 6), //
    WEBSITE_PROFILE("Website Profile", 7), //
    TECHNOLOGY_PROFILE("Technology Profile", 8), //
    ONLINE_PRESENCE("Online Presence", 9), //
    GROWTH_TRENDS("Growth Trends", 10), //
    WEBSITE_KEYWORDS("Website Keywords", 11), //
    ACCOUNT_INFORMATION("Account Information", 12), //
    LEAD_INFORMATION("Lead Information", 13), //
    DEFAULT("Default", 14), //
    AI_INSIGHTS("AI Insights", 15);

    private static Map<String, Category> nameMap;
    private static Set<String> values;
    private static List<Category> premiumCategories = Arrays.asList(INTENT, TECHNOLOGY_PROFILE,
            WEBSITE_KEYWORDS, ACCOUNT_ATTRIBUTES, CONTACT_ATTRIBUTES);
    // used in following scenarios
    // 1. iteration metadata API called via remodeling UI
    // 2. attribute management UI
    private static Set<Category> hiddenFromUiCategories = new HashSet<>(
            Arrays.asList(LEAD_INFORMATION, DEFAULT, ACCOUNT_INFORMATION));
    private static Set<Category> ldcReservedCategories = Sets.newHashSet(FIRMOGRAPHICS, //
            GROWTH_TRENDS, //
            INTENT, //
            ONLINE_PRESENCE, //
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
            throw new IllegalArgumentException("Cannot find a Category with name " + name);
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
