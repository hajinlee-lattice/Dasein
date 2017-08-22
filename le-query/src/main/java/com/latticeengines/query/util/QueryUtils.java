package com.latticeengines.query.util;

import java.util.ArrayList;
import java.util.List;

import com.latticeengines.domain.exposed.query.SubQuery;
import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.StringPath;

public final class QueryUtils {

    public static StringPath getAttributePath(BusinessEntity entity, String attrName) {
        return Expressions.stringPath(Expressions.stringPath(entity.name()), attrName);
    }

    public static StringPath getAttributePath(SubQuery subQuery, String attrName) {
        return Expressions.stringPath(Expressions.stringPath(subQuery.getAlias()), attrName);
    }

    public static List<Predicate> getJoinPredicates(BusinessEntity.Relationship relationship) {
        List<Predicate> joinPredicates = new ArrayList<>();
        for (Pair<InterfaceName, InterfaceName> pair : relationship.getJoinKeys()) {
            String srcAttrName = pair.getLeft().name();
            String tgtAttrName = pair.getRight().name();
            // ON T1.c1 = T2.c2
            StringPath sourceAttr = QueryUtils.getAttributePath(relationship.getParent(), srcAttrName);
            StringPath targetAttr = QueryUtils.getAttributePath(relationship.getChild(), tgtAttrName);
            joinPredicates.add(sourceAttr.eq(targetAttr));
        }
        return joinPredicates;
    }

}
