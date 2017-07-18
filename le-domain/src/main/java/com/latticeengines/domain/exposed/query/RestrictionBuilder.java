package com.latticeengines.domain.exposed.query;

import java.util.ArrayList;
import java.util.List;

import edu.emory.mathcs.backport.java.util.Arrays;

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
        if (restriction != null) {
            throw new IllegalArgumentException("Cannot chain a lookup here.");
        }
        if (existsEntity != null) {
            throw new IllegalArgumentException("Cannot specify let and exists together.");
        }
        attrLookup = new AttributeLookup(entity, attrName);
        complete = false;
        return this;
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

    public RestrictionBuilder eq(Object value) {
        operator = ComparisonType.EQUAL;
        rhsLookup = new ValueLookup(value);
        negate = false;
        completeConcrete();
        return this;
    }

    public RestrictionBuilder eq(BusinessEntity entity, String attrName) {
        operator = ComparisonType.EQUAL;
        rhsLookup = new AttributeLookup(entity, attrName);
        negate = false;
        completeConcrete();
        return this;
    }

    public RestrictionBuilder neq(Object value) {
        return this.not().eq(value);
    }

    public RestrictionBuilder neq(BusinessEntity entity, String attrName) {
        return this.not().eq(entity, attrName);
    }

    public RestrictionBuilder gte(Object min) {
        return in(min, null);
    }

    public RestrictionBuilder lt(Object max) {
        return in(null, max);
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

    @SuppressWarnings("unchecked")
    public RestrictionBuilder and(Restriction... children) {
        return and(new ArrayList<Restriction>(Arrays.asList(children)));
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

    @SuppressWarnings("unchecked")
    public RestrictionBuilder or(Restriction... children) {
        return or(new ArrayList<Restriction>(Arrays.asList(children)));
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
