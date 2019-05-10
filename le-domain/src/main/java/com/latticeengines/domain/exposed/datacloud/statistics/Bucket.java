package com.latticeengines.domain.exposed.datacloud.statistics;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.AggregationFilter;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.TimeFilter;

import io.swagger.annotations.ApiModel;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
@ApiModel("Represents of a bucket. Use Cmp and Vals fields for a normal bucket or Txn field for a transaction bucket. "
        + "If none is provided, consider as \"equals to the label\"")
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

    @JsonProperty("Lift")
    private Double lift;

    @JsonProperty("Cmp")
    private ComparisonType comparisonType;

    @JsonProperty("Vals")
    private List<Object> values;

    @JsonProperty("Txn")
    private Transaction transaction;

    @JsonProperty("Chg")
    private Change change;

    @JsonProperty("Fltr")
    private TimeFilter dateFilter;

    public Bucket() {
    }

    public Bucket(Bucket bucket) {
        // used for deep copy during stats calculation
        this();
        this.label = bucket.label;
        if (bucket.count != null) {
            this.count = bucket.count;
        }
        this.id = bucket.id;
        if (bucket.encodedCountList != null) {
            this.encodedCountList = new Long[bucket.encodedCountList.length];
            int idx = 0;
            for (Long cnt : bucket.encodedCountList) {
                this.encodedCountList[idx++] = cnt;
            }
        }
        if (bucket.lift != null) {
            this.lift = bucket.lift;
        }

        this.comparisonType = bucket.getComparisonType();
        this.values = bucket.getValues();
    }

    public static Bucket nullBkt() {
        Bucket bucket = new Bucket();
        bucket.setComparisonType(ComparisonType.IS_NULL);
        return bucket;
    }

    public static Bucket notNullBkt() {
        Bucket bucket = new Bucket();
        bucket.setComparisonType(ComparisonType.IS_NOT_NULL);
        return bucket;
    }

    public static Bucket rangeBkt(Object min, Object max) {
        return rangeBkt(min, max, true, false);
    }

    public static Bucket rangeBkt(Object min, Object max, boolean minInclusive, boolean maxInclusive) {
        Bucket bucket = new Bucket();
        List<Object> vals;
        ComparisonType comparator;
        if (min != null && max != null) {
            vals = Arrays.asList(min, max);
            if (minInclusive && maxInclusive) {
                if (min == max) {
                    comparator = ComparisonType.EQUAL;
                    vals = Collections.singletonList(min);
                } else {
                    comparator = ComparisonType.GTE_AND_LTE;
                }
            } else if (minInclusive) {
                comparator = ComparisonType.GTE_AND_LT;
            } else if (maxInclusive) {
                comparator = ComparisonType.GT_AND_LTE;
            } else {
                comparator = ComparisonType.GT_AND_LT;
            }
        } else if (min != null) {
            vals = Collections.singletonList(min);
            if (minInclusive) {
                comparator = ComparisonType.GREATER_OR_EQUAL;
            } else {
                comparator = ComparisonType.GREATER_THAN;
            }
        } else if (max != null) {
            vals = Collections.singletonList(max);
            if (maxInclusive) {
                comparator = ComparisonType.LESS_OR_EQUAL;
            } else {
                comparator = ComparisonType.LESS_THAN;
            }
        } else {
            throw new IllegalArgumentException("A bucket cannot have both min and max being null");
        }
        bucket.setComparisonType(comparator);
        bucket.setValues(vals);
        return bucket;
    }

    public static Bucket valueBkt(String value) {
        Bucket bucket = new Bucket();
        if (StringUtils.isNotBlank(value)) {
            bucket.setLabel(value);
            bucket.setComparisonType(ComparisonType.EQUAL);
            bucket.setValues(Collections.singletonList(value));
        } else {
            bucket.setComparisonType(ComparisonType.IS_NULL);
        }
        return bucket;
    }

    public static Bucket valueBkt(ComparisonType comparisonType, List<Object> values) {
        Bucket bucket = new Bucket();
        bucket.setComparisonType(comparisonType);
        bucket.setValues(values);
        return bucket;
    }

    public static Bucket txnBkt(Transaction txn) {
        Bucket bucket = new Bucket();
        bucket.setTransaction(txn);
        return bucket;
    }

    public static Bucket dateBkt(TimeFilter dateFilter) {
        Bucket bucket = new Bucket();
        bucket.setDateFilter(dateFilter);
        return bucket;
    }

    public static Bucket chgBkt(Change.Direction direction, Change.ComparisonType comparator, List<Object> values) {
        Bucket bucket = new Bucket();
        Change change = new Change();
        change.setDirection(direction);
        change.setComparisonType(comparator);
        change.setAbsVals(values);
        bucket.setChange(change);
        return bucket;
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

    public ComparisonType getComparisonType() {
        return comparisonType;
    }

    public void setComparisonType(ComparisonType comparisonType) {
        this.comparisonType = comparisonType;
    }

    public List<Object> getValues() {
        return values;
    }

    public void setValues(List<Object> values) {
        this.values = values;
    }

    public Double getLift() {
        return lift;
    }

    public void setLift(Double lift) {
        this.lift = lift;
    }

    public Transaction getTransaction() {
        return transaction;
    }

    public void setTransaction(Transaction transaction) {
        this.transaction = transaction;
    }

    public Change getChange() {
        return change;
    }

    public void setChange(Change change) {
        this.change = change;
    }

    public TimeFilter getDateFilter() {
        return dateFilter;
    }

    public void setDateFilter(TimeFilter dateFilter) {
        this.dateFilter = dateFilter;
    }

    public int getIdAsInt() {
        return id.intValue();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class Transaction implements Serializable {

        private static final long serialVersionUID = -5862461839904574785L;

        @JsonProperty("PrdId")
        private String productId;

        @JsonProperty("Time")
        private TimeFilter timeFilter;

        @JsonProperty("Negate")
        private Boolean negate;

        @JsonProperty("Amt")
        private AggregationFilter spentFilter;

        @JsonProperty("Qty")
        private AggregationFilter unitFilter;

        // for jackson
        @SuppressWarnings("unused")
        private Transaction() {
        }

        public Transaction(String productId, TimeFilter timeFilter, AggregationFilter spentFilter,
                AggregationFilter unitFilter, Boolean negate) {
            this.productId = productId;
            this.timeFilter = timeFilter;
            this.spentFilter = spentFilter;
            this.unitFilter = unitFilter;
            this.negate = negate;
        }

        public String getProductId() {
            return productId;
        }

        public TimeFilter getTimeFilter() {
            return timeFilter;
        }

        public Boolean getNegate() {
            return negate;
        }

        public AggregationFilter getSpentFilter() {
            return spentFilter;
        }

        public AggregationFilter getUnitFilter() {
            return unitFilter;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class Change implements Serializable {

        private static final long serialVersionUID = 5575780996889844007L;

        @JsonProperty("Direction")
        private Direction direction;

        @JsonProperty("Cmp")
        private ComparisonType comparisonType;

        @JsonProperty("Vals")
        private List<Object> absVals;

        public Direction getDirection() {
            return direction;
        }

        public void setDirection(Direction direction) {
            this.direction = direction;
        }

        public ComparisonType getComparisonType() {
            return comparisonType;
        }

        public void setComparisonType(ComparisonType comparisonType) {
            this.comparisonType = comparisonType;
        }

        public List<Object> getAbsVals() {
            return absVals;
        }

        public void setAbsVals(List<Object> absVals) {
            this.absVals = absVals;
        }

        public enum Direction {
            INC, DEC
        }

        public enum ComparisonType {
            AT_LEAST, AS_MUCH_AS, BETWEEN
        }
    }
}
