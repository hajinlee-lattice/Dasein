package com.latticeengines.domain.exposed.util;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.QueryBuilder;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.Sort;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndSort;
import com.latticeengines.domain.exposed.query.frontend.QueryDecorator;

public class QueryTranslator {
    private static final Logger log = LoggerFactory.getLogger(QueryTranslator.class);

    public static final int MAX_ROWS = 250;
    private static final PageFilter DEFAULT_PAGE_FILTER = new PageFilter(0, 100);

    public static Query translate(FrontEndQuery frontEndQuery, QueryDecorator decorator) {
        Restriction restriction = translateFrontEndRestriction(frontEndQuery.getRestriction());
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
                .freeText(frontEndQuery.getFreeFormTextSearch()) //
                .orderBy(translateFrontEndSort(frontEndQuery.getSort())) //
                .page(frontEndQuery.getPageFilter());

        if (decorator != null) {
            if (decorator.addSelects()) {
                queryBuilder.select(BusinessEntity.LatticeAccount, decorator.getLDCLookups());
                queryBuilder.select(decorator.getLookupEntity(), decorator.getEntityLookups());
            }
            queryBuilder.freeTextAttributes(decorator.getFreeTextSearchEntity(), decorator.getFreeTextSearchAttrs());
        }

        return queryBuilder.build();
    }

    public static Restriction translateFrontEndRestriction(FrontEndRestriction frontEndRestriction) {
        if (frontEndRestriction == null) {
            return null;
        }

        List<Restriction> restrictions = new ArrayList<>();

        List<Restriction> allRestrictions = frontEndRestriction.getAll().stream() //
                .map(BucketRestriction::convert).collect(Collectors.toList());
        Restriction and = Restriction.builder().and(allRestrictions).build();
        restrictions.add(and);

        List<Restriction> anyRestrictions = frontEndRestriction.getAny().stream() //
                .map(BucketRestriction::convert).collect(Collectors.toList());
        Restriction or = Restriction.builder().or(anyRestrictions).build();
        restrictions.add(or);

        if (frontEndRestriction.restrictNullSalesforceId()) {
            restrictions.add(Restriction.builder().let(BusinessEntity.Account, "SalesforceAccountID").isNull().build());
        } else if (frontEndRestriction.restrictNotNullSalesforceId()) {
            restrictions
                    .add(Restriction.builder().let(BusinessEntity.Account, "SalesforceAccountID").isNotNull().build());
        }

        return Restriction.builder().and(restrictions).build();
    }

    private static Sort translateFrontEndSort(FrontEndSort frontEndSort) {
        if (frontEndSort != null) {
            return new Sort(frontEndSort.getAttributes(), frontEndSort.getDescending());
        } else {
            return null;
        }
    }

}
