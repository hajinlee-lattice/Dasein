package com.latticeengines.query.evaluator.lookup;

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.BitCodecUtils;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.query.evaluator.QueryProcessor;
import com.latticeengines.query.exposed.exception.QueryEvaluationException;
import com.latticeengines.query.factory.sqlquery.BaseSQLQuery;
import com.latticeengines.query.util.QueryUtils;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.dsl.ComparableExpression;
import com.querydsl.core.types.dsl.Expressions;

public class AttributeResolver<T extends AttributeLookup> extends BaseLookupResolver<T> implements LookupResolver<T> {

    private static final Logger log = LoggerFactory.getLogger(AttributeResolver.class);

    protected QueryProcessor queryProcessor;
    protected String sqlUser;

    public AttributeResolver(AttributeRepository repository, QueryProcessor queryProcessor, String sqlUser) {
        super(repository);
        this.queryProcessor = queryProcessor;
        this.sqlUser = sqlUser;
    }

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
                return QueryUtils.getAttributePath(entity, cm.getAttrName());
            } else {
                String physicalColumnName = cm.getPhysicalName();
                Integer offset = cm.getBitOffset();
                if (offset == null) {
                    offset = 0;
                }
                long bitMask = BitCodecUtils.bitMask(0L, 0, numBits);
                log.info("Using bit mask " + bitMask + " and right shift " + offset + " to extract " + cm.getAttrName() + " from " + cm.getPhysicalName());
                BaseSQLQuery<?> query = queryProcessor.getQueryFactory().getQuery(repository, sqlUser);
                if (alias) {
                    return query.getBitEncodedExpression(QueryUtils.getAttributePath(entity, physicalColumnName),
                            offset, bitMask).as(Expressions.stringPath(cm.getAttrName()));
                } else {
                    return query.getBitEncodedExpression(QueryUtils.getAttributePath(entity, physicalColumnName),
                            offset, bitMask);
                }
            }
        } else {
            if (alias) {
                return QueryUtils.getAttributePath(entity, cm.getAttrName()).as(Expressions.stringPath(cm.getAttrName()));
            } else {
                return QueryUtils.getAttributePath(entity, cm.getAttrName());
            }
        }
    }

}
