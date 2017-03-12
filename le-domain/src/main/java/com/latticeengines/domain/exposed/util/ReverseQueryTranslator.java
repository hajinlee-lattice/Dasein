package com.latticeengines.domain.exposed.util;

import java.util.List;
import java.util.stream.Collectors;

import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.LogicalOperator;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;

public class ReverseQueryTranslator {
    private Query query;

    public ReverseQueryTranslator(Query query) {
        this.query = query;
    }

    public FrontEndQuery translate() {
        FrontEndQuery result = new FrontEndQuery();

        FrontEndRestriction restriction = translateRestriction(query.getRestriction());
        result.setRestriction(restriction);
        result.setFreeFormTextSearch(query.getFreeFormTextSearch());
        result.setPageFilter(query.getPageFilter());
        result.setSort(query.getSort());
        return result;
    }

    public static FrontEndRestriction translateRestriction(Restriction restriction) {
        if (restriction == null) {
            return null;
        }

        FrontEndRestriction result = new FrontEndRestriction();
        if (restriction instanceof LogicalRestriction) {
            LogicalRestriction casted = (LogicalRestriction) restriction;
            if (casted.getOperator() == LogicalOperator.AND) {
                List<Restriction> children = casted.getRestrictions();
                if (children.stream().allMatch(r -> r instanceof LogicalRestriction)) {
                    List<LogicalRestriction> castedChildren = children.stream().map(r -> (LogicalRestriction) r)
                            .collect(Collectors.toList());

                    LogicalRestriction and = castedChildren.stream()
                            .filter(r -> r.getOperator() == LogicalOperator.AND).findFirst().orElse(null);
                    LogicalRestriction or = castedChildren.stream().filter(r -> r.getOperator() == LogicalOperator.OR)
                            .findFirst().orElse(null);

                    if (and != null) {
                        List<BucketRestriction> andChildren = and.getRestrictions().stream()
                                .filter(r -> r instanceof BucketRestriction).map(r -> (BucketRestriction) r)
                                .collect(Collectors.toList());
                        result.setAll(andChildren);
                    }

                    if (or != null) {
                        List<BucketRestriction> orChildren = or.getRestrictions().stream()
                                .filter(r -> r instanceof BucketRestriction).map(r -> (BucketRestriction) r)
                                .collect(Collectors.toList());
                        result.setAny(orChildren);
                    }

                    return result;
                }
            }
        }

        throw new RuntimeException(String.format("Restriction is not in the correct format: %s", restriction));
    }
}
