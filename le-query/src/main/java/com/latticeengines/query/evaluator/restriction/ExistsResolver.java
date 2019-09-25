package com.latticeengines.query.evaluator.restriction;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ExistsRestriction;
import com.latticeengines.domain.exposed.query.JoinSpecification;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.query.exposed.exception.QueryEvaluationException;
import com.latticeengines.query.exposed.factory.QueryFactory;
import com.latticeengines.query.util.AttrRepoUtils;
import com.latticeengines.query.util.QueryUtils;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.Expressions;
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

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public BooleanExpression resolve(ExistsRestriction restriction) {
        if (restriction.getSubQueryExpression() == null) {
            BusinessEntity tgtEntity = restriction.getEntity();
            // find one-to-many relationships pointing to tgt entity
            JoinSpecification join = joins.stream() //
                    .filter(j -> j.getDestinationEntity().equals(tgtEntity)) //
                    .findAny().orElseThrow(() -> new QueryEvaluationException(
                            "Cannot find a proper join to construct exists clause for " + tgtEntity));
            BusinessEntity srcEntity = join.getSourceEntity();
            BusinessEntity.Relationship relationship = srcEntity.join(tgtEntity);
            List<Predicate> joinPredicates = QueryUtils.getJoinPredicates(relationship);
            StringPath mainTable = AttrRepoUtils.getTablePath(attrRepo, tgtEntity);
            SQLQuery<?> query = queryFactory.getQuery(attrRepo, getSqlUser()).from(mainTable.as(tgtEntity.name()));
            Restriction innerRestriction = restriction.getRestriction();
            BooleanExpression innerPredicate = null;
            if (innerRestriction != null) {
                RestrictionResolver innerResolver = factory.getRestrictionResolver(innerRestriction.getClass());
                innerPredicate = innerResolver.resolve(innerRestriction);
            }
            for (Predicate p : joinPredicates) {
                if (innerPredicate == null) {
                    innerPredicate = Expressions.asBoolean(p);
                } else {
                    innerPredicate = innerPredicate.and(p);
                }
            }
            query = query.where(innerPredicate);
            if (restriction.getNegate()) {
                return query.notExists();
            } else {
                return query.exists();
            }
        } else {
            SQLQuery<?> subQuery = (SQLQuery<?>) restriction.getSubQueryExpression();
            if (restriction.getNegate()) {
                return subQuery.notExists();
            } else {
                return subQuery.exists();
            }
        }
    }

}
