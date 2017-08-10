package com.latticeengines.domain.exposed.metadata;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
    DEFAULT("Default");

    private final String name;
    private static Map<String, Category> nameMap;
    private static Set<String> values;

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
        if (name == null) {
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

}
