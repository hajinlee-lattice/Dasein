package com.latticeengines.domain.exposed.cdl.activity;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CategorizeDoubleConfig extends CategorizeValConfig {

    private static final long serialVersionUID = 0L;

    public static final String NAME = "CategorizeDoubleConfig";

    @JsonProperty("categories")
    // categoryName -> <comparator -> bound>
    private Map<String, Map<Comparator, Double>> categories = new HashMap<>();

    @Override
    @JsonIgnore
    public Set<String> getCategoryNames() {
        return new HashSet<>(categories.keySet());
    }

    public Map<String, Map<Comparator, Double>> getCategories() {
        return categories;
    }

    public void setCategories(Map<String, Map<Comparator, Double>> categories) {
        this.categories = categories;
    }

    public String findCategory(Double val) {
        for (Map.Entry<String, Map<Comparator, Double>> category : categories.entrySet()) {
            String categoryName = category.getKey();
            Map<Comparator, Double> conditions = category.getValue();
            if (anyConditionMet(conditions, val)) {
                return categoryName;
            }
        }
        return CATEGORY_UNDEFINED;
    }

    private boolean anyConditionMet(Map<Comparator, Double> conditions, Double val) {
        return conditions.entrySet().stream().anyMatch(entry -> {
            Comparator comp = entry.getKey();
            double bound = entry.getValue();
            switch (comp) {
            case GE:
                return val >= bound;
            case GT:
                return val > bound;
            case EQ:
                return val == bound;
            case LT:
                return val < bound;
            case LE:
                return val <= bound;
            default:
                throw new UnsupportedOperationException(String.format("Comparator %s is not supported.", comp));
            }
        });
    }

    public enum Comparator {
        EQ, // equals
        GT, GE, // greater than
        LT, LE // less than
    }
}
