package com.latticeengines.domain.exposed.metadata.statistics;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.latticeengines.domain.exposed.metadata.Category;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class Statistics {

    @JsonProperty("Count")
    private Long count;

    @JsonProperty("Categories")
    @JsonDeserialize(keyUsing = CategoryKeyDeserializer.class)
    @JsonSerialize(keyUsing = CategoryKeySerializer.class)
    private Map<Category, CategoryStatistics> categories = new HashMap<>();

    public Map<Category, CategoryStatistics> getCategories() {
        return categories;
    }

    public void setCategories(Map<Category, CategoryStatistics> categories) {
        this.categories = categories;
    }
         
    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public CategoryStatistics getCategory(Category category) {
        return categories.get(category);
    }

    public void putCategory(Category category, CategoryStatistics cateStats) {
        categories.put(category, cateStats);
    }

    public boolean hasCategory(Category category) {
        return categories.containsKey(category);
    }

    private static class CategoryKeyDeserializer extends KeyDeserializer {
        public CategoryKeyDeserializer() {
        }

        @Override
        public Object deserializeKey(String key, DeserializationContext ctxt) throws IOException {
            return Category.fromName(key);
        }
    }

    private static class CategoryKeySerializer extends JsonSerializer<Category> {
        public CategoryKeySerializer() {
        }

        @Override
        public void serialize(Category value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
            jgen.writeFieldName(value.getName());
        }
    }
}
