package com.latticeengines.objectapi.util;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.QueryBuilder;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.ValueLookup;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.util.TimeFilterTranslator;
import com.latticeengines.query.exposed.factory.QueryFactory;

public class EntityQueryTranslator extends QueryTranslator {

    private static final Logger log = LoggerFactory.getLogger(EntityQueryTranslator.class);

    public EntityQueryTranslator(QueryFactory queryFactory, AttributeRepository repository) {
        super(queryFactory, repository);
    }

    public Query translateEntityQuery(FrontEndQuery frontEndQuery, boolean isCountQuery, //
            TimeFilterTranslator timeTranslator, String sqlUser) {
        BusinessEntity mainEntity = frontEndQuery.getMainEntity();

        if (BusinessEntity.Product.equals(mainEntity)) {
            return translateProductQuery(frontEndQuery, isCountQuery);
        }

        Restriction restriction = translateEntityQueryRestriction(frontEndQuery, timeTranslator, sqlUser);
        QueryBuilder queryBuilder = Query.builder();
        queryBuilder.from(mainEntity).where(restriction) //
                .orderBy(translateFrontEndSort(frontEndQuery.getSort())) //
                .page(frontEndQuery.getPageFilter()) //
                .distinct(frontEndQuery.getDistinct());

        if (CollectionUtils.isNotEmpty(frontEndQuery.getLookups())) {
            frontEndQuery.getLookups().forEach(queryBuilder::select);
        } else if (isCountQuery) {
            queryBuilder.select(new ValueLookup(1));
        }

        configurePagination(frontEndQuery);

        Query query = queryBuilder.build();
        if (MapUtils.isNotEmpty(frontEndQuery.getJoinHints())) {
            query.setJoinHints(frontEndQuery.getJoinHints());
        }
        return query;
    }

}
