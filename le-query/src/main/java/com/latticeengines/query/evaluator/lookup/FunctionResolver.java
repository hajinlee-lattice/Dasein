package com.latticeengines.query.evaluator.lookup;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.FunctionLookup;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.dsl.Expressions;

public class FunctionResolver<T, I> extends BaseLookupResolver<FunctionLookup<T, I>>
        implements LookupResolver<FunctionLookup<T, I>> {

    public FunctionResolver(AttributeRepository repository) {
        super(repository);
    }

    @Override
    public Expression<T> resolveForSelect(FunctionLookup<T, I> lookup, boolean asAlias) {
        if (asAlias) {
            return Expressions.simpleTemplate(lookup.getType(), lookup.getFunction().apply(lookup.getAgrs()))
                    .as(lookup.getAlias());
        }
        return Expressions.simpleTemplate(lookup.getType(), lookup.getFunction().apply(lookup.getAgrs()));
    }

}
