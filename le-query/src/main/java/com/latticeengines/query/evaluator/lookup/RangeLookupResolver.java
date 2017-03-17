package com.latticeengines.query.evaluator.lookup;

import java.util.ArrayList;
import java.util.List;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.RangeLookup;
import com.querydsl.core.types.dsl.ComparableExpression;
import com.querydsl.core.types.dsl.Expressions;

public class RangeLookupResolver extends LookupResolver {
    private RangeLookup lookup;

    public RangeLookupResolver(RangeLookup lookup, SchemaInterpretation rootObjectType, DataCollection dataCollection,
            Lookup secondaryLookup) {
        super(dataCollection, rootObjectType);
        this.lookup = lookup;
    }

    @Override
    public List<ComparableExpression<String>> resolve() {
        // TODO Handle bucketed attributes

        List<ComparableExpression<String>> expressions = new ArrayList<>();
        ComparableExpression<String> min = Expressions.asComparable(lookup.getMin().toString());
        ComparableExpression<String> max = Expressions.asComparable(lookup.getMax().toString());
        expressions.add(min);
        expressions.add(max);
        return expressions;
    }
}
