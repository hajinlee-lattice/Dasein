package com.latticeengines.domain.exposed.query;

import static com.latticeengines.domain.exposed.query.ComparisonType.EQUAL;
import static com.latticeengines.domain.exposed.query.ComparisonType.GREATER_OR_EQUAL;
import static com.latticeengines.domain.exposed.query.ComparisonType.IN_RANGE;
import static com.latticeengines.domain.exposed.query.ComparisonType.LESS_THAN;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;
import com.latticeengines.domain.exposed.query.frontend.FrontEndBucket;

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
    private FrontEndBucket bkt;

    public BucketRestriction(AttributeLookup attr, FrontEndBucket bkt) {
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

    public FrontEndBucket getBkt() {
        return bkt;
    }

    public void setBkt(FrontEndBucket bkt) {
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
    @SuppressWarnings("unchecked")
    public Map<String, Collection<? extends GraphNode>> getChildMap() {
        Map<String, Collection<? extends GraphNode>> map = new HashMap<>();
        map.put("attr", Collections.singletonList(attr));
        return map;
    }

    @Override
    public void accept(Visitor visitor, VisitorContext ctx) {
        visitor.visit(this, ctx);
    }

    public ConcreteRestriction convert() {
        if (bkt.getRange() == null && bkt.getValue() == null) {
            return new ConcreteRestriction(false, attr, EQUAL, null);
        } else if (bkt.getRange() != null) {
            return new ConcreteRestriction(false, attr, IN_RANGE, new RangeLookup(bkt.getRange().getLeft(), bkt.getRange().getRight()));
        } else {
            return new ConcreteRestriction(false, attr, EQUAL, new ValueLookup(bkt.getValue()));
        }
    }

    public static BucketRestriction from(Restriction restriction) {
        return from((ConcreteRestriction) restriction);
    }

    private static BucketRestriction from(ConcreteRestriction restriction) {
        if (restriction.getNegate()) {
            throw new IllegalArgumentException("Negate concrete restriction cannot be converted to bucket.");
        }
        AttributeLookup attributeLookup = (AttributeLookup) restriction.getLhs();
        FrontEndBucket bucket = new FrontEndBucket();
        BucketRestriction bucketRestriction = new BucketRestriction();
        bucketRestriction.setAttr(attributeLookup);
        bucketRestriction.setBkt(bucket);
        ComparisonType operator = restriction.getRelation();
        Lookup rhs = restriction.getRhs();
        switch (operator) {
            case EQUAL:
            case GREATER_OR_EQUAL:
            case LESS_THAN:
                if (rhs instanceof ValueLookup) {
                    ValueLookup valueLookup = (ValueLookup) rhs;
                    Object val = valueLookup.getValue();
                    if (EQUAL.equals(operator)) {
                        if (val != null && val instanceof String) {
                            bucket.setValue((String) val);
                            return bucketRestriction;
                        } else if (val == null) {
                            return bucketRestriction;
                        } else {
                            throw new IllegalArgumentException("Cannot handle non string value.");
                        }
                    } else if (GREATER_OR_EQUAL.equals(operator)) {
                        if (val != null && val instanceof Number) {
                            bucket.setRange(Pair.of(val, null));
                            return bucketRestriction;
                        } else {
                            throw new IllegalArgumentException(
                                    "Cannot convert greater than or equal to for the given value: " + val);
                        }
                    } else if (LESS_THAN.equals(operator)) {
                        if (val != null && val instanceof Number) {
                            bucket.setRange(Pair.of(null, val));
                            return bucketRestriction;
                        } else {
                            throw new IllegalArgumentException("Cannot convert less than for for the given value: " + val);
                        }
                    }
                } else {
                    throw new IllegalArgumentException("The right hand side of EQUAL must be a value.");
                }
            case IN_RANGE:
                if (rhs instanceof RangeLookup) {
                    RangeLookup rangeLookup = (RangeLookup) rhs;
                    Object min = rangeLookup.getMin();
                    Object max = rangeLookup.getMax();
                    Pair<Object, Object> range = Pair.of(min, max);
                    bucket.setRange(range);
                    return bucketRestriction;
                } else {
                    throw new IllegalArgumentException("Cannot convert to bucket.");
                }
            default:
                throw new IllegalArgumentException("Cannot convert " + operator);
        }
    }

}
