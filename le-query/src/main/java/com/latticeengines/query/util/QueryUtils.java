package com.latticeengines.query.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.query.SubQuery;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.dsl.NumberPath;
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

    public static NumberPath getAttributeNumberPath(BusinessEntity entity, String attrName) {
        return Expressions.numberPath(BigDecimal.class, Expressions.stringPath(entity.name()), attrName);
    }

    public static NumberPath getAttributeNumberPath(SubQuery subQuery, String attrName) {
        return Expressions.numberPath(BigDecimal.class, Expressions.stringPath(subQuery.getAlias()), attrName);
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
