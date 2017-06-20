package com.latticeengines.domain.exposed.datacloud.statistics;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
@ApiModel("Represents of a bucket. It can has a single value (Lbl) or a range (Rng) defined by a pair of boundaries. "
        + "If both are null, meaning it is a bucket for null value.")
public class Bucket implements Serializable {

    private static final long serialVersionUID = -8550825595883518157L;

    @JsonProperty("Lbl")
    private String label;

    @JsonProperty("Cnt")
    private Long count;

    @JsonProperty("Id")
    private Long id;

    @JsonProperty("En")
    private Long[] encodedCountList;

    @JsonIgnore
    private Pair<Object, Object> range;

    @JsonProperty("Lift")
    private Double lift;

    public static Bucket nullBkt() {
        return new Bucket();
    }

    public static Bucket rangeBkt(Object min, Object max) {
        Bucket bucket =  new Bucket();
        bucket.setRange(ImmutablePair.of(min, max));
        return bucket;
    }

    public static Bucket valueBkt(String value) {
        Bucket bucket =  new Bucket();
        if (StringUtils.isNotBlank(value)) {
            bucket.setLabel(value);
        }
        return bucket;
    }

    public Bucket() {
    }

    public Bucket(Bucket bucket) {
        // used for deep copy during stats calculation
        this();
        this.label = bucket.label;
        if (bucket.count != null) {
            this.count = new Long(bucket.count);
        }
        this.id = bucket.id;
        if (bucket.encodedCountList != null) {
            this.encodedCountList = new Long[bucket.encodedCountList.length];
            int idx = 0;
            for (Long cnt : bucket.encodedCountList) {
                this.encodedCountList[idx++] = new Long(cnt);
            }
        }
        if (bucket.range != null) {
            this.range = bucket.range;
        }
        if (bucket.lift != null) {
            this.lift = new Double(bucket.lift);
        }
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long[] getEncodedCountList() {
        return encodedCountList;
    }

    public void setEncodedCountList(Long[] encodedCountList) {
        this.encodedCountList = encodedCountList;
    }

    @JsonIgnore
    public Pair<Object, Object> getRange() {
        return range;
    }

    @JsonIgnore
    public void setRange(Pair<Object, Object> range) {
        this.range = range;
    }

    @JsonProperty("Rng")
    @ApiModelProperty("First value is the lower bound of the range, inclusive. Null is negative infinity. "
            + "Second value is the upper bound of the range, exclusive. Null is positive infinity. "
            + "Should not set both to null.")
    private List<Object> getRangeAsList() {
        return range == null ? null : Arrays.asList(range.getLeft(), range.getRight());
    }

    @JsonProperty("Rng")
    @ApiModelProperty("First value is the lower bound of the range, inclusive. Null is negative infinity. "
            + "Second value is the upper bound of the range, exclusive. Null is positive infinity. "
            + "Should not set both to null.")
    private void setRangeViaList(List<Object> objs) {
        if (objs != null) {
            this.range = ImmutablePair.of(objs.get(0), objs.get(1));
        }
    }

    public Double getLift() {
        return lift;
    }

    public void setLift(Double lift) {
        this.lift = lift;
    }

    public int getIdAsInt() {
        return id.intValue();
    }
}
