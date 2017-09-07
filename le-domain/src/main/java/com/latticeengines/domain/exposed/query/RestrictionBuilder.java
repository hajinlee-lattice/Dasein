package com.latticeengines.domain.exposed.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.latticeengines.common.exposed.util.JsonUtils;

public class RestrictionBuilder {

    private Restriction restriction;
    private boolean complete;

    private Lookup attrLookup;
    private ComparisonType operator;
    private Lookup rhsLookup;
    private boolean negate;

    private BusinessEntity existsEntity;

    private LogicalOperator logicalOperator;
    private List<Restriction> children;

    public RestrictionBuilder let(BusinessEntity entity, String attrName) {
        return let(new AttributeLookup(entity, attrName));
    }

    public RestrictionBuilder let(Lookup lookup) {
        if (lookup instanceof AttributeLookup || lookup instanceof SubQueryAttrLookup) {
            if (restriction != null) {
                throw new IllegalArgumentException("Cannot chain a lookup here.");
            }
            if (existsEntity != null) {
                throw new IllegalArgumentException("Cannot specify let and exists together.");
            }
            this.attrLookup = lookup;
            complete = false;
            return this;
        } else {
            throw new UnsupportedOperationException("Only support attribute lookup and sub query attr lookup.");
        }
    }

    public RestrictionBuilder let(BusinessEntity entity, String attrName, AggregateLookup.Aggregator aggregator) {
        return let(new AttributeLookup(entity, attrName), aggregator);
    }

    public RestrictionBuilder let(Lookup lookup, AggregateLookup.Aggregator aggregator) {
        if (lookup instanceof AttributeLookup || lookup instanceof SubQueryAttrLookup) {
            if (restriction != null) {
                throw new IllegalArgumentException("Cannot chain a lookup here.");
            }
            if (existsEntity != null) {
                throw new IllegalArgumentException("Cannot specify let and exists together.");
            }
            this.attrLookup = AggregateLookup.create(lookup, aggregator);
            complete = false;
            return this;
        } else {
            throw new UnsupportedOperationException("Only support aggregation of attr and sub query attr lookup.");
        }
    }

    public RestrictionBuilder exists(BusinessEntity entity) {
        if (restriction != null) {
            throw new IllegalArgumentException("Cannot chain a lookup here.");
        }
        if (attrLookup != null) {
            throw new IllegalArgumentException("Cannot specify let and exists together.");
        }
        existsEntity = entity;
        complete = false;
        return this;
    }

    public RestrictionBuilder that(Restriction innerRestriction) {
        if (complete) {
            throw new IllegalArgumentException("The builder is already complete. Check your operation chaining.");
        }
        restriction = new ExistsRestriction(existsEntity, false, innerRestriction);
        complete = true;
        return this;
    }

    public RestrictionBuilder not() {
        negate = true;
        complete = false;
        return this;
    }

    public RestrictionBuilder not(ConcreteRestriction restriction) {
        negate = true;
        this.restriction = JsonUtils.deserialize(JsonUtils.serialize(restriction), ConcreteRestriction.class);
        ((ConcreteRestriction) this.restriction).setNegate(true);
        complete = true;
        return this;
    }

    public RestrictionBuilder eq(Object value) {
        operator = ComparisonType.EQUAL;
        rhsLookup = new ValueLookup(value);
        completeConcrete();
        return this;
    }

    public RestrictionBuilder eq(BusinessEntity entity, String attrName) {
        operator = ComparisonType.EQUAL;
        rhsLookup = new AttributeLookup(entity, attrName);
        completeConcrete();
        return this;
    }

    public RestrictionBuilder neq(Object value) {
        return this.not().eq(value);
    }

    public RestrictionBuilder neq(BusinessEntity entity, String attrName) {
        return this.not().eq(entity, attrName);
    }

    public RestrictionBuilder gt(Object value) {
        operator = ComparisonType.GREATER_THAN;
        negate = false;
        rhsLookup = new ValueLookup(value);
        completeConcrete();
        return this;
    }

    public RestrictionBuilder gte(Object min) {
        return in(min, null);
    }

    public RestrictionBuilder lt(Object max) {
        return in(null, max);
    }

    public RestrictionBuilder lte(Object value) {
        operator = ComparisonType.LESS_OR_EQUAL;
        negate = false;
        rhsLookup = new ValueLookup(value);
        completeConcrete();
        return this;
    }

    public RestrictionBuilder inCollection(Collection<Object> collection) {
        if (collection == null) {
            throw new IllegalArgumentException("collection cannot be null");
        }
        if (collection.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("object in collection cannot be null.");
        }
        operator = ComparisonType.IN_COLLECTION;
        negate = false;
        rhsLookup = new CollectionLookup(collection);
        completeConcrete();
        return this;
    }

    public RestrictionBuilder inCollection(SubQuery subQuery, String subQueryAttrName) {
        if (subQuery == null) {
            throw new IllegalArgumentException("subquery cannot be null");
        }
        if (subQueryAttrName == null) {
            throw new IllegalArgumentException("subquery attribute name cannot be null");
        }
        operator = ComparisonType.IN_COLLECTION;
        negate = false;
        rhsLookup = new SubQueryAttrLookup(subQuery, subQueryAttrName);
        completeConcrete();
        return this;
    }

    public RestrictionBuilder in(Object min, Object max) {
        if (min == null && max == null) {
            throw new RuntimeException("min and max cannot both be null.");
        }
        if (min != null && max != null) {
            operator = ComparisonType.IN_RANGE;
            rhsLookup = new RangeLookup(min, max);
        } else if (min != null) {
            operator = ComparisonType.GREATER_OR_EQUAL;
            rhsLookup = new ValueLookup(min);
        } else {
            operator = ComparisonType.LESS_THAN;
            rhsLookup = new ValueLookup(max);
        }
        negate = false;
        completeConcrete();
        return this;
    }

    public RestrictionBuilder isNull() {
        operator = ComparisonType.IS_NULL;
        negate = false;
        completeConcrete();
        return this;
    }

    public RestrictionBuilder isNotNull() {
        operator = ComparisonType.IS_NOT_NULL;
        negate = false;
        completeConcrete();
        return this;
    }

    public RestrictionBuilder and(RestrictionBuilder... childBuilders) {
        List<Restriction> children = new ArrayList<>();
        for (RestrictionBuilder builder: childBuilders) {
            children.add(builder.build());
        }
        return and(children);
    }

    public RestrictionBuilder and(Restriction... children) {
        return and(Arrays.stream(children).filter(Objects::nonNull).collect(Collectors.toList()));
    }

    public RestrictionBuilder and(List<Restriction> children) {
        logicalOperator = LogicalOperator.AND;
        this.children = children;
        completeLogical();
        return this;
    }

    public RestrictionBuilder or(RestrictionBuilder... childBuilders) {
        List<Restriction> children = new ArrayList<>();
        for (RestrictionBuilder builder: childBuilders) {
            children.add(builder.build());
        }
        return or(children);
    }

    public RestrictionBuilder or(Restriction... children) {
        return or(Arrays.stream(children).filter(Objects::nonNull).collect(Collectors.toList()));
    }

    public RestrictionBuilder or(List<Restriction> children) {
        logicalOperator = LogicalOperator.OR;
        this.children = children;
        completeLogical();
        return this;
    }

    public Restriction build() {
        if (!complete) {
            throw new IllegalArgumentException("Restriction definition is incomplete.");
        }
        return restriction;
    }

    private void completeConcrete() {
        if (attrLookup == null) {
            throw new IllegalArgumentException("Must define left hand side lookup first.");
        }
        if (complete) {
            throw new IllegalArgumentException("The builder is already complete. Check your operation chaining.");
        }
        restriction = new ConcreteRestriction(negate, attrLookup, operator, rhsLookup);
        complete = true;
    }

    private void completeLogical() {
        if (attrLookup != null || existsEntity != null) {
            throw new IllegalArgumentException("Cannot put logical operation in concrete restriction, it should be outside.");
        }
        if (complete) {
            throw new IllegalArgumentException("The builder is already complete. Check your operation chaining.");
        }
        restriction = new LogicalRestriction(logicalOperator, children);
        complete = true;
    }

}
