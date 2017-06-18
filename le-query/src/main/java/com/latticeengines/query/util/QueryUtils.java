package com.latticeengines.query.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.query.exposed.exception.QueryEvaluationException;
import com.querydsl.core.types.EntityPath;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.PathBuilder;
import com.querydsl.core.types.dsl.StringPath;

public final class QueryUtils {

    public static boolean testmode = false;

    public static StringPath getAttributePath(BusinessEntity entity, String attrName) {
        return Expressions.stringPath(Expressions.stringPath(entity.name()), attrName);
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

    public static List<Predicate> getJoinPredicates(BusinessEntity.Relationship relationship) {
        List<Predicate> joinPredicates = new ArrayList<>();
        for (Pair<InterfaceName, InterfaceName> pair : relationship.getJoinKeys()) {
            String srcAttrName = pair.getLeft().name();
            String tgtAttrName = pair.getRight().name();
            // TODO: after fixing test data to use interface name, we should
            // remove this hack
            if (QueryUtils.testmode) {
                srcAttrName = QueryUtils.renameJoinKeyForTest(srcAttrName);
                tgtAttrName = QueryUtils.renameJoinKeyForTest(tgtAttrName);
            }
            // ON T1.c1 = T2.c2
            StringPath sourceAttr = QueryUtils.getAttributePath(relationship.getParent(), srcAttrName);
            StringPath targetAttr = QueryUtils.getAttributePath(relationship.getChild(), tgtAttrName);
            joinPredicates.add(sourceAttr.eq(targetAttr));
        }
        return joinPredicates;
    }

    public static String renameJoinKeyForTest(String interfaceName) {
        return "id";
    }

}
