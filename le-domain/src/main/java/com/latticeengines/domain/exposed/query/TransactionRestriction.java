package com.latticeengines.domain.exposed.query;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class TransactionRestriction extends Restriction {

    @JsonProperty("productId")
    private String productId;

    @JsonProperty("targetProductId")
    private String targetProductId;

    @JsonProperty("timeFilter")
    private TimeFilter timeFilter;

    @JsonProperty("negate")
    private boolean negate;

    @JsonProperty("spentFilter")
    private AggregationFilter spentFilter;

    @JsonProperty("unitFilter")
    private AggregationFilter unitFilter;

    public TransactionRestriction() {
    }

    public TransactionRestriction(String productId, TimeFilter timeFilter, boolean negate, //
                                  AggregationFilter spentFilter, AggregationFilter unitFilter) {
        this.productId = productId;
        this.timeFilter = timeFilter;
        this.negate = negate;
        this.spentFilter = spentFilter;
        this.unitFilter = unitFilter;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getTargetProductId() {
        return targetProductId;
    }

    public void setTargetProductId(String targetProductId) {
        this.targetProductId = targetProductId;
    }

    public TimeFilter getTimeFilter() {
        return timeFilter;
    }

    public void setTimeFilter(TimeFilter timeFilter) {
        this.timeFilter = timeFilter;
    }

    public boolean isNegate() {
        return negate;
    }

    public void setNegate(boolean negate) {
        this.negate = negate;
    }

    public AggregationFilter getSpentFilter() {
        return spentFilter;
    }

    public void setSpentFilter(AggregationFilter spentFilter) {
        if (spentFilter != null && AggregationSelector.SPENT != spentFilter.getSelector()) {
            throw new IllegalArgumentException("Invalid aggregation selector for spent filter. Valid type should be " +
                                               AggregationSelector.SPENT);
        }
        this.spentFilter = spentFilter;
    }

    public AggregationFilter getUnitFilter() {
        return unitFilter;
    }

    public void setUnitFilter(AggregationFilter unitFilter) {
        if (unitFilter != null && AggregationSelector.UNIT != unitFilter.getSelector()) {
            throw new IllegalArgumentException("Invalid aggregation selector for unit filter. Valid type should be " +
                                               AggregationSelector.UNIT);
        }
        this.unitFilter = unitFilter;
    }
}
