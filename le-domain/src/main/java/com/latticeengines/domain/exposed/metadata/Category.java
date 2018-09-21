package com.latticeengines.domain.exposed.metadata;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public enum Category {
    FIRMOGRAPHICS("Firmographics"), //
    GROWTH_TRENDS("Growth Trends"), //
    INTENT("Intent"), //
    LEAD_INFORMATION("Lead Information"), //
    ACCOUNT_INFORMATION("Account Information"), //
    ONLINE_PRESENCE("Online Presence"), //
    TECHNOLOGY_PROFILE("Technology Profile"), //
    WEBSITE_KEYWORDS("Website Keywords"), //
    WEBSITE_PROFILE("Website Profile"), //
    ACCOUNT_ATTRIBUTES("My Attributes"), //
    CONTACT_ATTRIBUTES("Contact Attributes"), //
    PRODUCT_SPEND("Product Spend Profile"), //
    RATING("Lattice Ratings"), //
    CURATED_ACCOUNT_ATTRIBUTES("Curated Account Attributes"), //
    DEFAULT("Default");

    private final String name;
    private static Map<String, Category> nameMap;
    private static Set<String> values;
    private static List<Category> premiumCategories = Arrays.asList(INTENT, TECHNOLOGY_PROFILE, WEBSITE_KEYWORDS,
            ACCOUNT_ATTRIBUTES, CONTACT_ATTRIBUTES);
    private static Set<Category> hiddenFromUiCategories = new HashSet<>(
            Arrays.asList(LEAD_INFORMATION, DEFAULT, ACCOUNT_INFORMATION));
    private static Set<Category> ldcReservedCategories = Sets.newHashSet(
            FIRMOGRAPHICS, //
            GROWTH_TRENDS, //
            INTENT, //
            ONLINE_PRESENCE, //
            TECHNOLOGY_PROFILE, //
            WEBSITE_KEYWORDS, //
            WEBSITE_PROFILE //
    );

    static {
        nameMap = new HashMap<>();
        for (Category category : Category.values()) {
            nameMap.put(category.getName(), category);
        }
        values = new HashSet<>(Arrays.stream(values()).map(Category::name).collect(Collectors.toSet()));
    }

    Category(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public String toString() {
        return this.name;
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
        public void serialize(Category value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
            jgen.writeFieldName(value.getName());
        }
    }

    public static List<Category> getPremiunCategories() {
        return premiumCategories;
    }

    public boolean isHiddenFromUi() {
        return hiddenFromUiCategories.contains(this);
    }

    public boolean isPremium() {
        return premiumCategories.contains(this);
    }

}
