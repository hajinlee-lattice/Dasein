package com.latticeengines.domain.exposed.query.frontend;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
@ApiModel("Represents of a bucket. It can has a single value or a range defined by a pair of boundaries. "
        + "If both are null, meaning it is a bucket for null value.")
public class FrontEndBucket {

    @JsonIgnore
    @ApiModelProperty("First value is the lower bound of the range, inclusive. Null is negative infinity. "
            + "Second value is the upper bound of the range, exclusive. Null is positive infinity. "
            + "Should not set both to null.")
    private Pair<Object, Object> range;

    @JsonProperty("value")
    private Object value;

    public static FrontEndBucket range(Object min, Object max) {
        FrontEndBucket bucket = new FrontEndBucket();
        bucket.range = Pair.of(min, max);
        return bucket;
    }

    public static FrontEndBucket value(Object value) {
        FrontEndBucket bucket = new FrontEndBucket();
        bucket.value = value;
        return bucket;
    }

    public static FrontEndBucket nullBkt() {
        return new FrontEndBucket();
    }

    public FrontEndBucket() {
    }

    @JsonIgnore
    public Pair<Object, Object> getRange() {
        return range;
    }

    @JsonIgnore
    public void setRange(Pair<Object, Object> range) {
        this.range = range;
    }

    @JsonProperty("range")
    private List<Object> getRangeAsList() {
        return range == null ? null : Arrays.asList(range.getLeft(), range.getRight());
    }

    @JsonProperty("range")
    private void setRangeViaList(List<Object> objs) {
        if (objs != null) {
            this.range = Pair.of(objs.get(0), objs.get(1));
        }
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

}
