package com.latticeengines.query.util;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.SubQuery;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.NumberPath;
import com.querydsl.core.types.dsl.StringPath;

public final class QueryUtils {

    public static StringPath getAttributePath(String attrName) {
        return Expressions.stringPath(attrName);
    }

    public static StringPath getAttributePath(BusinessEntity entity, String attrName) {
        return getAttributePath(entity.name(), attrName);
    }

    public static StringPath getAttributePath(SubQuery subQuery, String attrName) {
        return getAttributePath(subQuery.getAlias(), attrName);
    }

    public static StringPath getAttributePath(String alias, String attrName) {
        return Expressions.stringPath(Expressions.stringPath(alias), attrName);
    }

    public static NumberPath getAttributeNumberPath(BusinessEntity entity, String attrName) {
        return Expressions.numberPath(BigDecimal.class, Expressions.stringPath(entity.name()), attrName);
    }

    public static NumberPath getAttributeNumberPath(SubQuery subQuery, String attrName) {
        return Expressions.numberPath(BigDecimal.class, Expressions.stringPath(subQuery.getAlias()), attrName);
    }

    public static List<Predicate> getJoinPredicates(BusinessEntity.Relationship relationship) {
        return getJoinPredicates(relationship, Collections.emptyMap());
    }

    public static List<Predicate> getJoinPredicates(BusinessEntity.Relationship relationship,
                                                    Map<BusinessEntity, String> entityAliasMap) {
        List<Predicate> joinPredicates = new ArrayList<>();
        for (Pair<InterfaceName, InterfaceName> pair : relationship.getJoinKeys()) {
            String srcAttrName = pair.getLeft().name();
            String tgtAttrName = pair.getRight().name();
            // ON T1.c1 = T2.c2
            String parentAlias = entityAliasMap.get(relationship.getParent());
            String childAlias = entityAliasMap.get(relationship.getChild());
            StringPath sourceAttr = (parentAlias == null) ?
                    QueryUtils.getAttributePath(relationship.getParent(), srcAttrName):
                    QueryUtils.getAttributePath(parentAlias, srcAttrName);
            StringPath targetAttr = (childAlias == null) ?
                    QueryUtils.getAttributePath(relationship.getChild(), tgtAttrName):
                    QueryUtils.getAttributePath(childAlias, tgtAttrName);
            joinPredicates.add(sourceAttr.eq(targetAttr));
        }
        return joinPredicates;
    }

}
