package com.latticeengines.domain.exposed.query;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@ApiModel("Represents a range of values, or a single value, either categorical or numerical.")
public class BucketRange {

    @ApiModelProperty("Minimum value of the range.  If this is a single value, min and max are set to that value.  "
            + "Null is negative infinity.")
    @JsonProperty("min")
    private Object min;

    @ApiModelProperty("Maximum value of the range.  If this is a single value, min and max are set to that value.  "
            + "Null is positive infinity.")
    @JsonProperty("max")
    private Object max;

    public BucketRange(Object min, Object max) {
        this.min = min;
        this.max = max;
    }

    public BucketRange(Object val) {
        this.min = this.max = val;
    }

    public BucketRange() {
    }

    public Object getMin() {
        return min;
    }

    public void setMin(Object min) {
        this.min = min;
    }

    public Object getMax() {
        return max;
    }

    public void setMax(Object max) {
        this.max = max;
    }
}
