package com.latticeengines.query.evaluator.lookup;

import java.util.Collections;
import java.util.List;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.FunctionLookup;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.dsl.ComparableExpression;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.SimpleExpression;

public class FunctionResolver<T extends Comparable<?>, I, O> extends BaseLookupResolver<FunctionLookup<T, I, O>>
        implements LookupResolver<FunctionLookup<T, I, O>> {

    private LookupResolverFactory factory;

    public FunctionResolver(AttributeRepository repository, LookupResolverFactory factory) {
        super(repository);
        this.factory = factory;
    }

    @Override
    public Expression<T> resolveForSelect(FunctionLookup<T, I, O> lookup, boolean asAlias) {
        return resolve(lookup, asAlias);
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public List<ComparableExpression<? extends Comparable<?>>> resolveForCompare(FunctionLookup<T, I, O> lookup) {
        return Collections.singletonList(Expressions.asComparable(resolve(lookup, false)));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private Expression<T> resolve(FunctionLookup<T, I, O> lookup, boolean asAlias) {
        SimpleExpression<T> expr = null;

        LookupResolver resolver = factory.getLookupResolver(lookup.getLookup().getClass());
        List<I> paths = resolver.resolveForCompare(lookup.getLookup());
        lookup.setArgs(paths.get(0));

        O output = lookup.getFunction().apply(lookup.getAgrs());
        if (output instanceof String) {
            expr = Expressions.simpleTemplate(lookup.getType(), (String) output);
        } else {
            expr = (SimpleExpression<T>) output;
        }
        if (asAlias) {
            return expr.as(lookup.getAlias());
        }
        return expr;
    }

}
