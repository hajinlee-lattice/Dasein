package com.latticeengines.domain.exposed.query;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class AggregationFilter {

    @JsonProperty("Selector")
    private AggregationSelector selector;

    @JsonProperty("Agg")
    private AggregationType aggregationType;

    @JsonProperty("Cmp")
    private ComparisonType comparisonType;

    @JsonProperty("Vals")
    private List<Object> values;

    // include not purchased for less comparison, used for playmaker migration
    @JsonProperty("includeNotPurchased")
    private boolean includeNotPurchased;

    AggregationFilter() {
    }

    public AggregationFilter(ComparisonType comparisonType, List<Object> values) {
        this.comparisonType = comparisonType;
        this.values = values;
        this.includeNotPurchased = false;
    }

    public AggregationFilter(AggregationSelector selector, AggregationType aggregationType,
            ComparisonType comparisonType, List<Object> values) {
        this(selector, aggregationType, comparisonType, values, false);
    }

    public AggregationFilter(AggregationSelector selector, AggregationType aggregationType,
            ComparisonType comparisonType, List<Object> values, boolean includeNotPurchased) {
        this.selector = selector;
        this.aggregationType = aggregationType;
        this.comparisonType = comparisonType;
        this.values = values;
        this.includeNotPurchased = includeNotPurchased;
    }

    public AggregationSelector getSelector() {
        return selector;
    }

    public AggregationType getAggregationType() {
        return aggregationType;
    }

    public ComparisonType getComparisonType() {
        return comparisonType;
    }

    public List<Object> getValues() {
        return values;
    }

    public boolean isIncludeNotPurchased() {
        return includeNotPurchased;
    }
}
