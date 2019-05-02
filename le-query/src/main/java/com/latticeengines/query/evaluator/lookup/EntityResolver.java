package com.latticeengines.query.evaluator.lookup;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.EntityLookup;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.dsl.ComparableExpression;
import com.querydsl.core.types.dsl.Expressions;

public class EntityResolver extends BaseLookupResolver<EntityLookup> implements LookupResolver<EntityLookup> {

    EntityResolver(AttributeRepository repository) {
        super(repository);
    }

    @Override
    public List<ComparableExpression<? extends Comparable<?>>> resolveForCompare(EntityLookup lookup) {
        throw new UnsupportedOperationException("Should not use entity look in where clause");
    }

    public Expression<?> resolveForSelect(EntityLookup lookup, boolean asAlias) {
        return Expressions.as(Expressions.constant(1), lookup.getEntity().name());
    }

}
