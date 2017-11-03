package com.latticeengines.query.evaluator.lookup;

import java.util.Collections;
import java.util.List;

import com.latticeengines.common.exposed.util.BitCodecUtils;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.query.exposed.exception.QueryEvaluationException;
import com.latticeengines.query.util.QueryUtils;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.dsl.ComparableExpression;
import com.querydsl.core.types.dsl.Expressions;

public class AttributeResolver<T extends AttributeLookup> extends BaseLookupResolver<T> implements LookupResolver<T> {

    public AttributeResolver(AttributeRepository repository) {
        super(repository);
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public List<ComparableExpression<? extends Comparable<?>>> resolveForCompare(AttributeLookup lookup) {
        if (lookup.getEntity() == null) {
            return Collections.singletonList(QueryUtils.getAttributePath(lookup.getAttribute()));
        }
        ColumnMetadata cm = getColumnMetadata(lookup);
        if (cm == null) {
            throw new IllegalArgumentException("Cannot find the attribute " + lookup + " in attribute repository.");
        }
        return Collections.singletonList((ComparableExpression<? extends Comparable<?>>) resolveBucketRange(lookup.getEntity(), cm, false));
    }

    @Override
    public Expression<?> resolveForSelect(AttributeLookup lookup, boolean asAlias) {
        if (lookup.getEntity() == null) {
            return QueryUtils.getAttributePath(lookup.getAttribute());
        }
        ColumnMetadata cm = getColumnMetadata(lookup);
        if (cm == null) {
            throw new QueryEvaluationException("Cannot find the attribute " + lookup + " in attribute repository.");
        }
        return resolveBucketRange(lookup.getEntity(), cm, asAlias);
    }

    private Expression<String> resolveBucketRange(BusinessEntity entity, ColumnMetadata cm, boolean alias) {
        AttributeStats stats = cm.getStats();
        if (stats != null) {
            Integer numBits = cm.getNumBits();
            if (numBits == null) {
                return QueryUtils.getAttributePath(entity, cm.getName());
            } else {
                String physicalColumnName = cm.getPhysicalName();
                Integer offset = cm.getBitOffset();
                if (offset == null) {
                    offset = 0;
                }
                long bitMask = BitCodecUtils.bitMask(0L, offset, numBits);
                if (alias) {
                    return Expressions
                            .stringTemplate("({0}&{1})>>{2}", QueryUtils.getAttributePath(entity, physicalColumnName), //
                                    bitMask, //
                                    offset)
                            .as(Expressions.stringPath(cm.getName()));
                } else {
                    return Expressions.stringTemplate("({0}&{1})>>{2}",
                            QueryUtils.getAttributePath(entity, physicalColumnName), //
                            bitMask, //
                            offset);
                }
            }
        } else {
            if (alias) {
                return QueryUtils.getAttributePath(entity, cm.getName()).as(Expressions.stringPath(cm.getName()));
            } else {
                return QueryUtils.getAttributePath(entity, cm.getName());
            }
        }
    }

}
