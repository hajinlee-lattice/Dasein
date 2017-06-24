package com.latticeengines.domain.exposed.util;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.Sort;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndSort;

public class QueryTranslator {
    private static final Log log = LogFactory.getLog(QueryTranslator.class);

    private static final int MAX_ROWS = 250;
    private static final PageFilter DEFAULT_PAGE_FILTER = new PageFilter(0, 100);

    public static Query translate(FrontEndQuery frontEndQuery) {
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
        return Query.builder().where(restriction) //
                .freeText(frontEndQuery.getFreeFormTextSearch()) //
                .orderBy(translateFrontEndSort(frontEndQuery.getSort())) //
                .page(frontEndQuery.getPageFilter()).build();
    }

    public static Restriction translateFrontEndRestriction(FrontEndRestriction frontEndRestriction) {
        if (frontEndRestriction == null) {
            return null;
        }
        List<Restriction> allRestrictions = frontEndRestriction.getAll().stream() //
                .map(BucketRestriction::convert).collect(Collectors.toList());
        Restriction and = Restriction.builder().and(allRestrictions).build();

        List<Restriction> anyRestrictions = frontEndRestriction.getAny().stream() //
                .map(BucketRestriction::convert).collect(Collectors.toList());
        Restriction or = Restriction.builder().or(anyRestrictions).build();

        return Restriction.builder().and(and, or).build();
    }

    private static Sort translateFrontEndSort(FrontEndSort frontEndSort) {
        if (frontEndSort != null) {
            return new Sort(frontEndSort.getAttributes(), frontEndSort.getDescending());
        } else {
            return null;
        }
    }

}
