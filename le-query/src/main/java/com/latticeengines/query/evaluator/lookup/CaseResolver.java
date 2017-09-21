package com.latticeengines.query.evaluator.lookup;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.CaseLookup;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.util.RestrictionOptimizer;
import com.latticeengines.query.evaluator.restriction.RestrictionResolver;
import com.latticeengines.query.evaluator.restriction.RestrictionResolverFactory;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.dsl.CaseBuilder;
import com.querydsl.core.types.dsl.ComparableExpression;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.StringExpression;

public class CaseResolver extends BaseLookupResolver<CaseLookup> implements LookupResolver<CaseLookup> {

    private final RestrictionResolverFactory factory;

    public CaseResolver(AttributeRepository repository, RestrictionResolverFactory restrictionResolverFactory) {
        super(repository);
        this.factory = restrictionResolverFactory;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<ComparableExpression<? extends Comparable>> resolveForCompare(CaseLookup lookup) {
        throw new UnsupportedOperationException("Using case lookup in where clause is not supported.");
    }

    @SuppressWarnings("unchecked")
    @Override
    public Expression<?> resolveForSelect(CaseLookup lookup, boolean asAlias) {
        CaseBuilder caseBuilder = new CaseBuilder();
        CaseBuilder.Cases<String, StringExpression> cases = null;
        for (String key : lookup.getCaseMap().keySet()) {
            Restriction restriction = lookup.getCaseMap().get(key);
            restriction = RestrictionOptimizer.optimize(restriction);
            if (restriction != null) {
                RestrictionResolver resolver = factory.getRestrictionResolver(restriction.getClass());
                Predicate predicate = resolver.resolve(restriction);
                if (cases == null) {
                    cases = caseBuilder.when(predicate).then(key);
                } else {
                    cases = cases.when(predicate).then(key);
                }
            }
        }
        if (cases == null) {
            return asAlias ? Expressions.asString(lookup.getDefaultCase()).as(lookup.getAlias())
                    : Expressions.asString(lookup.getDefaultCase());
        } else {
            return asAlias ? cases.otherwise(lookup.getDefaultCase()).as(lookup.getAlias())
                    : cases.otherwise(lookup.getDefaultCase());
        }
    }

}
