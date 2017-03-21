package com.latticeengines.domain.exposed.query;

import org.apache.commons.lang.builder.EqualsBuilder;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
@ApiModel("Represents a range of values, or a single value, either categorical or numerical.  "
        + "All ranges are inclusive, like SQL between statements.")
public class BucketRange {

    @ApiModelProperty("Whether this range only includes null values.  Otherwise nulls aren't allowed in a BucketRange.")
    @JsonProperty("is_null_only")
    private boolean isNullOnly;

    @ApiModelProperty("Minimum value of the range.  If this is a single value, min and max are set to that value.  "
            + "Null is negative infinity.")
    @JsonProperty("min")
    private Object min;

    @ApiModelProperty("Maximum value of the range.  If this is a single value, min and max are set to that value.  "
            + "Null is positive infinity.")
    @JsonProperty("max")
    private Object max;

    public static BucketRange range(Object min, Object max) {
        BucketRange range = new BucketRange();
        range.setMin(min);
        range.setMax(max);
        return range;
    }

    public static BucketRange value(Object val) {
        BucketRange range = new BucketRange();
        range.setMin(val);
        range.setMax(val);
        return range;
    }

    public static BucketRange nullBucket() {
        BucketRange range = new BucketRange();
        range.setNullOnly(true);
        return range;
    }

    public BucketRange() {
    }

    @Override
    public boolean equals(Object range) {
        return EqualsBuilder.reflectionEquals(this, range);
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

    @JsonIgnore
    public boolean isNullOnly() {
        return isNullOnly;
    }

    @JsonIgnore
    public void setNullOnly(boolean nullOnly) {
        isNullOnly = nullOnly;
    }

    public String toString() {
        return JsonUtils.serialize(this);
    }
}
