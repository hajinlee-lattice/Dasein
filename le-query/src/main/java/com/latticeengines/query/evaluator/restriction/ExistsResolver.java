package com.latticeengines.query.evaluator.restriction;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ExistsRestriction;
import com.latticeengines.domain.exposed.query.JoinSpecification;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.query.exposed.exception.QueryEvaluationException;
import com.latticeengines.query.exposed.factory.QueryFactory;
import com.latticeengines.query.util.QueryUtils;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.StringPath;
import com.querydsl.sql.SQLQuery;

public class ExistsResolver extends BaseRestrictionResolver<ExistsRestriction>
        implements RestrictionResolver<ExistsRestriction> {

    private List<JoinSpecification> joins;
    private QueryFactory queryFactory;
    private AttributeRepository attrRepo;

    ExistsResolver(RestrictionResolverFactory factory) {
        super(factory);
        this.joins = factory.getExistsJoins();
        this.queryFactory = factory.getQueryFactory();
        this.attrRepo = getAttrRepo();
    }

    @SuppressWarnings("unchecked")
    @Override
    public BooleanExpression resolve(ExistsRestriction restriction) {
        BusinessEntity tgtEntity = restriction.getEntity();
        // find one-to-many relationships pointing to tgt entity
        JoinSpecification join = joins.stream() //
                .filter(j -> j.getDestinationEntity().equals(tgtEntity)) //
                .findAny().orElse(null);
        if (join ==  null) {
            throw new QueryEvaluationException("Cannot find a proper join to construct exists clause for " + tgtEntity);
        }
        BusinessEntity srcEntity = join.getSourceEntity();
        BusinessEntity.Relationship relationship = srcEntity.join(tgtEntity);
        List<Predicate> joinPredicates = QueryUtils.getJoinPredicates(relationship);
        StringPath mainTable = QueryUtils.getTablePath(attrRepo, tgtEntity);
        SQLQuery<?> query = queryFactory.getQuery(attrRepo).from(mainTable.as(tgtEntity.name()));
        Restriction innerRestriction = restriction.getRestriction();
        RestrictionResolver innerResolver = factory.getRestrictionResolver(innerRestriction.getClass());
        BooleanExpression innerPredicate = innerResolver.resolve(innerRestriction);
        for (Predicate p: joinPredicates) {
            innerPredicate = innerPredicate.and(p);
        }
        query = query.where(innerPredicate);
        if (restriction.getNegate()) {
            return query.notExists();
        } else {
            return query.exists();
        }
    }

}
