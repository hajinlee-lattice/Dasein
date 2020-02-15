package com.latticeengines.objectapi.util;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.QueryBuilder;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.ValueLookup;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.util.TimeFilterTranslator;
import com.latticeengines.objectapi.service.TempListService;
import com.latticeengines.query.exposed.factory.QueryFactory;

public class EntityQueryTranslator extends QueryTranslator {

    public EntityQueryTranslator(QueryFactory queryFactory, AttributeRepository repository, String sqlUser,
            TimeFilterTranslator timeFilterTranslator, TempListService tempListService) {
        super(queryFactory, repository, sqlUser, timeFilterTranslator, tempListService);
    }

    public Query translateEntityQuery(FrontEndQuery frontEndQuery, boolean isCountQuery) {
        BusinessEntity mainEntity = frontEndQuery.getMainEntity();

        if (BusinessEntity.Product.equals(mainEntity)) {
            return translateProductQuery(frontEndQuery, isCountQuery);
        }
        Restriction restriction = translateEntityQueryRestriction(frontEndQuery);
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
