package com.latticeengines.domain.exposed.query;

import static com.latticeengines.domain.exposed.query.ComparisonType.IS_NOT_NULL;
import static com.latticeengines.domain.exposed.query.ComparisonType.IS_NULL;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;

/**
 * This restriction is only needed by front end
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class BucketRestriction extends Restriction {

    @JsonIgnore
    private AttributeLookup attr;

    @JsonProperty("bkt")
    private Bucket bkt;

    public BucketRestriction(AttributeLookup attr, Bucket bkt) {
        this.attr = attr;
        this.bkt = bkt;
    }

    public BucketRestriction() {
    }

    public AttributeLookup getAttr() {
        return attr;
    }

    public void setAttr(AttributeLookup attr) {
        this.attr = attr;
    }

    public Bucket getBkt() {
        return bkt;
    }

    public void setBkt(Bucket bkt) {
        this.bkt = bkt;
    }

    // to simplify UI json. other restrictions does not need this.
    @JsonProperty("attr")
    private String getAttrAsString() {
        return attr == null ? null : attr.toString();
    }

    @JsonProperty("attr")
    private void setAttrViaString(String attr) {
        this.attr = AttributeLookup.fromString(attr);
    }

    @Override
    public Collection<? extends GraphNode> getChildren() {
        List<GraphNode> children = new ArrayList<>();
        children.add(attr);
        return children;
    }

    @Override
    public Map<String, Collection<? extends GraphNode>> getChildMap() {
        Map<String, Collection<? extends GraphNode>> map = new HashMap<>();
        map.put("attr", Collections.singletonList(attr));
        return map;
    }

    @Override
    public void accept(Visitor visitor, VisitorContext ctx) {
        visitor.visit(this, ctx);
    }

    // from UI to backend
    public Restriction convert() {
        if (bkt == null) {
            throw new IllegalArgumentException("cannot convert null bucket restriction");
        }

        ComparisonType comparisonType = bkt.getComparisonType();
        List<Object> values = bkt.getValues();

        if (comparisonType == null) {
            return convertBucketRange();
        } else {
            return convertValueComparisons(comparisonType, values);
        }
    }

    private Restriction convertValueComparisons(ComparisonType comparisonType, List<Object> values) {
        Restriction restriction = null;
        switch (comparisonType) {
        case IS_NULL:
            restriction = new ConcreteRestriction(false, attr, IS_NULL, null);
            break;
        case IS_NOT_NULL:
            restriction = new ConcreteRestriction(false, attr, IS_NOT_NULL, null);
            break;
        case EQUAL:
            validateSingleValue(values);
            restriction = Restriction.builder().let(attr).eq(values.get(0)).build();
            break;
        case NOT_EQUAL:
            validateSingleValue(values);
            restriction = Restriction.builder().let(attr).neq(values.get(0)).build();
            break;
        case GREATER_THAN:
            validateSingleValue(values);
            restriction = Restriction.builder().let(attr).gt(values.get(0)).build();
            break;
        case GREATER_OR_EQUAL:
            validateSingleValue(values);
            restriction = Restriction.builder().let(attr).gte(values.get(0)).build();
            break;
        case LESS_THAN:
            validateSingleValue(values);
            restriction = Restriction.builder().let(attr).lt(values.get(0)).build();
            break;
        case LESS_OR_EQUAL:
            validateSingleValue(values);
            restriction = Restriction.builder().let(attr).lte(values.get(0)).build();
            break;
        case IN_RANGE:
        case GTE_AND_LTE:
        case GT_AND_LTE:
        case GTE_AND_LT:
        case GT_AND_LT:
            validateInRangeValues(values);
            Object min = values.get(0);
            Object max = values.get(1);
            switch (comparisonType) {
                case GTE_AND_LTE:
                    restriction = Restriction.builder().and( //
                            Restriction.builder().let(attr).gte(min).build(),
                            Restriction.builder().let(attr).lte(max).build()
                    ).build();
                    break;
                case GT_AND_LTE:
                    restriction = Restriction.builder().and( //
                            Restriction.builder().let(attr).gt(min).build(),
                            Restriction.builder().let(attr).lte(max).build()
                    ).build();
                    break;
                case GTE_AND_LT:
                    restriction = Restriction.builder().and( //
                            Restriction.builder().let(attr).gte(min).build(),
                            Restriction.builder().let(attr).lt(max).build()
                    ).build();
                    break;
                case GT_AND_LT:
                    restriction = Restriction.builder().and( //
                            Restriction.builder().let(attr).gt(min).build(),
                            Restriction.builder().let(attr).lt(max).build()
                    ).build();
                    break;
                default:
                    restriction = Restriction.builder().let(attr).in(min, max).build();
                    break;
            }
            break;
        case IN_COLLECTION:
            restriction = Restriction.builder().let(attr).inCollection(values).build();
            break;
        case CONTAINS:
            restriction = Restriction.builder().let(attr).contains(values.get(0)).build();
            break;
        case NOT_CONTAINS:
            restriction = Restriction.builder().let(attr).notcontains(values.get(0)).build();
            break;
        case STARTS_WITH:
            restriction = Restriction.builder().let(attr).not().startsWith(values.get(0)).build();
            break;
        default:
            throw new UnsupportedOperationException("comparator " + comparisonType + " is not supported yet");
        }
        return restriction;
    }

    private void validateSingleValue(List<Object> values) {
        if (values == null || values.size() != 1) {
            throw new IllegalArgumentException("collection should have one value");
        }
    }

    private void validateInRangeValues(List<Object> values) {
        if (values == null || values.size() != 2) {
            throw new IllegalArgumentException("range should contain both min and max value");
        }
    }

    private Restriction convertBucketRange() {
        if (bkt.getRange() == null && StringUtils.isBlank(bkt.getLabel())) {
            return new ConcreteRestriction(false, attr, IS_NULL, null);
        } else if (bkt.getRange() != null) {
            if (bkt.getRange().getLeft() != null && bkt.getRange().getRight() != null) {
                if (bkt.getRange().getLeft().equals(bkt.getRange().getRight())) {
                    return Restriction.builder().let(attr).eq(bkt.getRange().getLeft()).build();
                } else {
                    Restriction lowerBound = Restriction.builder().let(attr).gte(bkt.getRange().getLeft()).build();
                    Restriction upperBound = Restriction.builder().let(attr).lt(bkt.getRange().getRight()).build();
                    return Restriction.builder().and(lowerBound, upperBound).build();
                }
            } else if (bkt.getRange().getLeft() != null) {
                return Restriction.builder().let(attr).gte(bkt.getRange().getLeft()).build();
            } else if (bkt.getRange().getRight() != null) {
                return Restriction.builder().let(attr).lt(bkt.getRange().getRight()).build();
            } else {
                throw new IllegalArgumentException("A range cannot have both boundaries null.");
            }
        } else {
            return Restriction.builder().let(attr).eq(bkt.getLabel()).build();
        }
    }

}
