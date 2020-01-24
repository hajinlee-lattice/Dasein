package com.latticeengines.query.util;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.query.exposed.exception.QueryEvaluationException;
import com.querydsl.core.types.EntityPath;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.PathBuilder;
import com.querydsl.core.types.dsl.StringPath;

public final class AttrRepoUtils {

    protected AttrRepoUtils() {
        throw new UnsupportedOperationException();
    }

    public static ColumnMetadata getAttribute(AttributeRepository attrRepo, AttributeLookup attributeLookup) {
        ColumnMetadata cm = attrRepo.getColumnMetadata(attributeLookup);
        if (cm == null) {
            throw new QueryEvaluationException("Cannot find attribute " + attributeLookup + " in the repository.");
        }
        cm.setAttrName(attributeLookup.getAttribute());
        return cm;
    }

    public static StringPath getTablePath(String tableName) {
        return Expressions.stringPath(tableName);
    }

    public static StringPath getTablePath(AttributeRepository repository, BusinessEntity entity) {
        String tableName = getTableName(repository, entity);
        return Expressions.stringPath(tableName);
    }

    public static EntityPath<String> getTablePathBuilder(AttributeRepository repository, BusinessEntity entity) {
        String tableName = getTableName(repository, entity);
        return new PathBuilder<>(String.class, tableName);
    }

    private static String getTableName(AttributeRepository repository, BusinessEntity entity) {
        TableRoleInCollection tableRole = entity.getServingStore();
        if (tableRole == null) {
            throw new QueryEvaluationException("Cannot find a serving store for " + entity);
        }
        String tableName = repository.getTableName(tableRole);
        if (tableName == null) {
            throw new QueryEvaluationException("Cannot find table of role " + tableRole + " in the repository.");
        }
        return tableName;
    }

}
