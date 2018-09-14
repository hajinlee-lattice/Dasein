package com.latticeengines.query.evaluator.lookup;

import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.SubQueryAttrLookup;
import com.latticeengines.domain.exposed.query.WindowFunctionLookup;
import com.latticeengines.query.util.QueryUtils;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.dsl.NumberExpression;
import com.querydsl.sql.SQLExpressions;
import com.querydsl.sql.WindowFunction;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class WindowFunctionResolver extends BaseLookupResolver<WindowFunctionLookup>
        implements LookupResolver<WindowFunctionLookup> {
    private LookupResolverFactory factory;

    public WindowFunctionResolver(AttributeRepository repository, LookupResolverFactory factory) {
        super(repository);
        this.factory = factory;
    }

    @Override
    public Expression<?> resolveForSelect(WindowFunctionLookup lookup, boolean asAlias) {
        switch (lookup.getFunctionType()) {
        case SUM:
            return windowExpressionForSelect(lookup, asAlias);
        case MAX:
            return windowExpressionForSelect(lookup, asAlias);
        case MIN:
            return windowExpressionForSelect(lookup, asAlias);
        default:
            throw new RuntimeException("Unsupported window function " + lookup.getFunctionType());
        }
    }

    private Expression<?> windowExpressionForSelect(WindowFunctionLookup lookup, boolean asAlias) {
        if (lookup.getTarget() == null) {
            throw new RuntimeException("Target is not specified for window function.");
        }

        Lookup target = lookup.getTarget();
        NumberExpression targetExpression = null;
        if (lookup.getTarget() instanceof AttributeLookup) {
            AttributeLookup innerLookup = (AttributeLookup) target;
            ColumnMetadata cm = getColumnMetadata(innerLookup);
            targetExpression = QueryUtils.getAttributeNumberPath(innerLookup.getEntity(), cm.getAttrName());
        } else if (target instanceof SubQueryAttrLookup) {
            SubQueryAttrLookup innerLookup = (SubQueryAttrLookup) target;
            targetExpression = QueryUtils.getAttributeNumberPath(innerLookup.getSubQuery(), innerLookup.getAttribute());
        }
        // note, querydsl does not support window function over case expression, though
        // it's ok with redshift

        if (targetExpression == null) {
            throw new UnsupportedOperationException(
                    "Window function is not supported for " + target.getClass().getName());
        }

        WindowFunction windowFunction = windowFunction(targetExpression, lookup.getFunctionType());
        windowFunction.partitionBy(resolvePartitionBy(lookup.getPartitionBy()));

        return (asAlias && StringUtils.isNotBlank(lookup.getAlias())) ? windowFunction.as(lookup.getAlias())
                : windowFunction;
    }

    private WindowFunction windowFunction(NumberExpression targetExpression,
            WindowFunctionLookup.FunctionType functionType) {
        switch (functionType) {
        case SUM:
            return SQLExpressions.sum(targetExpression).over();
        case AVG:
            return SQLExpressions.avg(targetExpression).over();
        case MAX:
            return SQLExpressions.max(targetExpression).over();
        case MIN:
            return SQLExpressions.min(targetExpression).over();
        default:
            throw new UnsupportedOperationException("Unsupported window function " + functionType);
        }
    }

    private Expression<?>[] resolvePartitionBy(List<Lookup> partitionLookups) {
        Expression<?>[] expressions = partitionLookups.stream().map(this::resolvePartitionBy).filter(Objects::nonNull)
                .toArray(Expression<?>[]::new);

        return expressions;
    }

    private Expression<?> resolvePartitionBy(Lookup partitionLookup) {
        Expression<?> expression = null;
        if (partitionLookup instanceof AttributeLookup) {
            AttributeLookup attrLookup = (AttributeLookup) partitionLookup;
            LookupResolver resolver = factory.getLookupResolver(attrLookup.getClass());
            expression = resolver.resolveForSelect(attrLookup, false);
        } else if (partitionLookup instanceof SubQueryAttrLookup) {
            SubQueryAttrLookup attrLookup = (SubQueryAttrLookup) partitionLookup;
            LookupResolver resolver = factory.getLookupResolver(attrLookup.getClass());
            expression = resolver.resolveForSelect(attrLookup, false);
        }
        return expression;
    }
}
