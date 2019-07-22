package com.latticeengines.objectapi.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.QueryBuilder;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.util.TimeFilterTranslator;
import com.latticeengines.query.exposed.factory.QueryFactory;

public class EntityQueryTranslator extends QueryTranslator {

    private static final Logger log = LoggerFactory.getLogger(EntityQueryTranslator.class);

    public EntityQueryTranslator(QueryFactory queryFactory, AttributeRepository repository) {
        super(queryFactory, repository);
    }

    public Query translateEntityQuery(FrontEndQuery frontEndQuery, QueryDecorator decorator, //
            TimeFilterTranslator timeTranslator, String sqlUser) {
        BusinessEntity mainEntity = frontEndQuery.getMainEntity();

        if (BusinessEntity.Product.equals(mainEntity)) {
            return translateProductQuery(frontEndQuery, decorator);
        }

        Restriction restriction = translateEntityQueryRestriction(frontEndQuery, timeTranslator, sqlUser);
        QueryBuilder queryBuilder = Query.builder();
        queryBuilder.from(mainEntity).where(restriction) //
                .orderBy(translateFrontEndSort(frontEndQuery.getSort())) //
                .page(frontEndQuery.getPageFilter()) //
                .distinct(frontEndQuery.getDistinct());

        if (CollectionUtils.isNotEmpty(frontEndQuery.getLookups())) {
            frontEndQuery.getLookups().forEach(queryBuilder::select);
        } else if (decorator != null && !decorator.isDataQuery()) {
            queryBuilder.select(decorator.getIdLookup());
        }

        if (decorator != null && StringUtils.isNotBlank(frontEndQuery.getFreeFormTextSearch())) {
            List<AttributeLookup> attrs = new ArrayList<>();
            for (AttributeLookup attributeLookup : decorator.getFreeTextSearchAttrs()) {
                if (repository != null && repository.getColumnMetadata(attributeLookup) != null) {
                    attrs.add(attributeLookup);
                }
            }
            if (CollectionUtils.isNotEmpty(attrs)) {
                queryBuilder.freeText(frontEndQuery.getFreeFormTextSearch(), attrs.toArray(new AttributeLookup[0]));
            } else {
                log.warn("None of the free text search attributes exists in attr repo, skip free text search.");
            }
        }

        configurePagination(frontEndQuery);

        Query query = queryBuilder.build();
        if (MapUtils.isNotEmpty(frontEndQuery.getJoinHints())) {
            query.setJoinHints(frontEndQuery.getJoinHints());
        }
        return query;
    }

}
