package com.latticeengines.objectapi.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.graph.traversal.impl.BreadthFirstSearch;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.QueryBuilder;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.Sort;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndSort;
import com.latticeengines.domain.exposed.util.RestrictionOptimizer;

public class QueryTranslator {
    private static final Logger log = LoggerFactory.getLogger(QueryTranslator.class);

    public static final int MAX_ROWS = 250;
    private static final PageFilter DEFAULT_PAGE_FILTER = new PageFilter(0, 100);

    public static Query translate(FrontEndQuery frontEndQuery, QueryDecorator decorator) {
        Restriction restriction = translateFrontEndRestriction(frontEndQuery.getFrontEndRestriction());
        if (frontEndQuery.restrictNullSalesforceId()) {
            Restriction sfidRestriction = Restriction.builder().let(BusinessEntity.Account, "SalesforceAccountID")
                    .isNull().build();
            restriction = Restriction.builder().and(restriction, sfidRestriction).build();
        } else if (frontEndQuery.restrictNotNullSalesforceId()) {
            Restriction sfidRestriction = Restriction.builder().let(BusinessEntity.Account, "SalesforceAccountID")
                    .isNotNull().build();
            restriction = Restriction.builder().and(restriction, sfidRestriction).build();
        }

        if (frontEndQuery.getPageFilter() == null) {
            frontEndQuery.setPageFilter(DEFAULT_PAGE_FILTER);
        } else {
            if (frontEndQuery.getPageFilter().getNumRows() > MAX_ROWS) {
                log.warn(String.format("Refusing to accept a query requesting more than %s rows."
                        + " Currently specified page filter: %s", MAX_ROWS, frontEndQuery.getPageFilter()));
                frontEndQuery.getPageFilter().setNumRows(MAX_ROWS);
            }
        }

        QueryBuilder queryBuilder = Query.builder().where(restriction) //
                .orderBy(translateFrontEndSort(frontEndQuery.getSort())) //
                .page(frontEndQuery.getPageFilter());

        if (frontEndQuery.getLookups() != null && !frontEndQuery.getLookups().isEmpty()) {
            frontEndQuery.getLookups().forEach(lookup -> {
                AttributeLookup attributeLookup = (AttributeLookup) lookup;
                queryBuilder.select(attributeLookup.getEntity(), attributeLookup.getAttribute());
            });
        } else if (decorator != null) {
            if (decorator.addSelects()) {
                queryBuilder.select(BusinessEntity.LatticeAccount, decorator.getLDCLookups());
                queryBuilder.select(decorator.getLookupEntity(), decorator.getEntityLookups());
            }
            queryBuilder.freeText(frontEndQuery.getFreeFormTextSearch(), decorator.getFreeTextSearchEntity(),
                    decorator.getFreeTextSearchAttrs());
        }

        return queryBuilder.build();
    }

    private static Restriction translateFrontEndRestriction(FrontEndRestriction frontEndRestriction) {
        if (frontEndRestriction == null || frontEndRestriction.getRestriction() == null) {
            return null;
        }

        Restriction restriction = frontEndRestriction.getRestriction();

        Restriction translated;
        if (restriction instanceof LogicalRestriction) {
            BreadthFirstSearch search = new BreadthFirstSearch();
            search.run(restriction, (object, ctx) -> {
                if (object instanceof BucketRestriction) {
                    BucketRestriction bucket = (BucketRestriction) object;
                    Restriction concrete = bucket.convert();
                    LogicalRestriction parent = (LogicalRestriction) ctx.getProperty("parent");
                    parent.getRestrictions().remove(bucket);
                    parent.getRestrictions().add(concrete);
                }
            });
            translated = restriction;
        } else {
            BucketRestriction bucket = (BucketRestriction) restriction;
            translated = bucket.convert();
        }
        return RestrictionOptimizer.optimize(translated);
    }

    private static Sort translateFrontEndSort(FrontEndSort frontEndSort) {
        if (frontEndSort != null) {
            return new Sort(frontEndSort.getAttributes(), frontEndSort.getDescending());
        } else {
            return null;
        }
    }

}
