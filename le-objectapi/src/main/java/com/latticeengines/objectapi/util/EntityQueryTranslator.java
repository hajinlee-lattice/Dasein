package com.latticeengines.objectapi.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.QueryBuilder;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.util.TimeFilterTranslator;
import com.latticeengines.query.exposed.factory.QueryFactory;

public class EntityQueryTranslator extends QueryTranslator {

    @SuppressWarnings("unused")
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

        Restriction restriction;
        QueryBuilder queryBuilder = Query.builder();

        restriction = translateFrontEndRestriction(getEntityFrontEndRestriction(mainEntity, frontEndQuery),
                                                   timeTranslator, sqlUser, true);
        restriction = translateSalesforceIdRestriction(frontEndQuery, mainEntity, restriction);
        restriction = translateInnerRestriction(frontEndQuery, mainEntity, restriction, timeTranslator, sqlUser);

        queryBuilder.from(mainEntity).where(restriction) //
                .orderBy(translateFrontEndSort(frontEndQuery.getSort())) //
                .page(frontEndQuery.getPageFilter()) //
                .distinct(frontEndQuery.getDistinct());

        if (CollectionUtils.isNotEmpty(frontEndQuery.getLookups())) {
            frontEndQuery.getLookups().forEach(lookup -> {
                AttributeLookup attributeLookup = (AttributeLookup) lookup;
                queryBuilder.select(attributeLookup.getEntity(), attributeLookup.getAttribute());
            });
        } else if (decorator != null && !decorator.isDataQuery()) {
            queryBuilder.select(decorator.getIdLookup());
        }

        if (decorator != null) {
            List<AttributeLookup> attrs = new ArrayList<>();
            for (AttributeLookup attributeLookup: decorator.getFreeTextSearchAttrs()) {
                if (repository.getColumnMetadata(attributeLookup) != null) {
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

        return queryBuilder.build();
    }

    private Restriction translateInnerRestriction(FrontEndQuery frontEndQuery, BusinessEntity outerEntity,
            Restriction outerRestriction, TimeFilterTranslator timeTranslator, String sqlUser) {
        BusinessEntity innerEntity = null;
        switch (outerEntity) {
        case Contact:
            innerEntity = BusinessEntity.Account;
            break;
        case Account:
            innerEntity = BusinessEntity.Contact;
            break;
        default:
            break;
        }
        FrontEndRestriction innerFrontEndRestriction = getEntityFrontEndRestriction(innerEntity, frontEndQuery);
        Restriction innerRestriction = translateFrontEndRestriction(innerFrontEndRestriction, timeTranslator, sqlUser,
                                                                    true);
        return addSubselectRestriction(outerEntity, outerRestriction, innerEntity, innerRestriction);
    }

}
