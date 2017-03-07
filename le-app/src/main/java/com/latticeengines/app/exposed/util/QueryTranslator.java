package com.latticeengines.app.exposed.util;

import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.LogicalOperator;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;

public class QueryTranslator {
    FrontEndQuery frontEndQuery;
    SchemaInterpretation objectType;

    public QueryTranslator(FrontEndQuery frontEndQuery, SchemaInterpretation objectType) {
        this.frontEndQuery = frontEndQuery;
        this.objectType = objectType;
    }

    public Query translate() {
        Query result = new Query();
        result.setObjectType(objectType);

        Restriction restriction = translateRestrictions();
        result.setRestriction(restriction);
        result.setFreeFormTextSearch(frontEndQuery.getFreeFormTextSearch());
        result.setPageFilter(frontEndQuery.getPageFilter());
        if (frontEndQuery.getPageFilter() == null) {
            frontEndQuery.setPageFilter(new PageFilter(0, 100));
        }
        result.setSort(frontEndQuery.getSort());
        return result;
    }

    private Restriction translateRestrictions() {
        if (frontEndQuery.getRestriction() == null) {
            return null;
        }

        LogicalRestriction parent = new LogicalRestriction();
        parent.setOperator(LogicalOperator.AND);

        LogicalRestriction and = new LogicalRestriction();
        and.setOperator(LogicalOperator.AND);

        LogicalRestriction or = new LogicalRestriction();
        or.setOperator(LogicalOperator.OR);

        parent.addRestriction(or);
        parent.addRestriction(and);

        and.getRestrictions().addAll(frontEndQuery.getRestriction().getAll());
        or.getRestrictions().addAll(frontEndQuery.getRestriction().getAny());
        return parent;
    }
}
