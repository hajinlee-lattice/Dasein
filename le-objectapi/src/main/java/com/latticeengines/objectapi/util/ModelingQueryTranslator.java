package com.latticeengines.objectapi.util;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.latticeengines.common.exposed.graph.traversal.impl.BreadthFirstSearch;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EventType;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.QueryBuilder;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.Sort;
import com.latticeengines.domain.exposed.query.TransactionRestriction;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.util.TimeFilterTranslator;
import com.latticeengines.query.exposed.factory.QueryFactory;
import com.latticeengines.query.exposed.translator.EventQueryTranslator;

public class ModelingQueryTranslator extends QueryTranslator {

    public ModelingQueryTranslator(QueryFactory queryFactory, AttributeRepository repository) {
        super(queryFactory, repository);
    }

    public Query translateModelingEvent(EventFrontEndQuery frontEndQuery, EventType eventType,
                                        TimeFilterTranslator timeTranslator) {

        if (restrictionNotSpecified(frontEndQuery) && restrictionNotSpecified(frontEndQuery.getSegmentQuery())) {
            throw new IllegalArgumentException("No restriction specified for event query");
        }

        FrontEndRestriction frontEndRestriction = getEntityFrontEndRestriction(BusinessEntity.Account, frontEndQuery);
        EventQueryTranslator eventQueryTranslator = new EventQueryTranslator();
        QueryBuilder queryBuilder = Query.builder();
        Restriction restriction = translateFrontEndRestriction(frontEndRestriction);
        restriction = translateInnerRestriction(frontEndQuery, BusinessEntity.Account, restriction);

        if (frontEndQuery.getSegmentQuery() != null) {
            Restriction segmentRestriction = translateSegmentQuery(
                frontEndQuery.getSegmentQuery(), timeTranslator, queryBuilder);
            restriction = Restriction.builder().and(segmentRestriction, restriction).build();
        }

        setTargetProducts(restriction, frontEndQuery.getTargetProductIds());

        switch (eventType) {
            case Scoring:
                queryBuilder = eventQueryTranslator.translateForScoring(queryFactory, repository, restriction,
                        frontEndQuery,
                        queryBuilder);
                break;
            case Training:
                queryBuilder = eventQueryTranslator.translateForTraining(queryFactory, repository, restriction,
                        frontEndQuery,
                        queryBuilder);
                break;
            case Event:
                queryBuilder = eventQueryTranslator.translateForEvent(queryFactory, repository, restriction,
                        frontEndQuery,
                        queryBuilder);
                break;
        }

        PageFilter pageFilter = new PageFilter(0, 0);

        if (frontEndQuery.getPageFilter() != null) {
            pageFilter = frontEndQuery.getPageFilter();
        }

        queryBuilder.page(pageFilter);

        if (pageFilter.getRowOffset() > 0 || pageFilter.getNumRows() > 0) {
            // set sort order, or pagination will return different result each time
            Sort sort = new Sort();
            sort.setLookups(queryBuilder.getLookups());
            queryBuilder.orderBy(sort);
        }

        return queryBuilder.build();
    }

    private boolean restrictionNotSpecified(FrontEndQuery frontEndQuery) {
        if (frontEndQuery == null) {
            return true;
        }
        FrontEndRestriction frontEndRestriction = getEntityFrontEndRestriction(BusinessEntity.Account, frontEndQuery);
        return frontEndRestriction == null || frontEndRestriction.getRestriction() == null;
    }

    private Restriction translateSegmentQuery(FrontEndQuery segmentQuery,
                                              TimeFilterTranslator timeTranslator,
                                              QueryBuilder queryBuilder) {
        FrontEndRestriction segmentAccountRestriction = getEntityFrontEndRestriction(BusinessEntity.Account,
                                                                                     segmentQuery);
        Restriction segmentRestriction = translateFrontEndRestriction(
            BusinessEntity.Account, segmentAccountRestriction, queryBuilder, timeTranslator);
        segmentRestriction = translateInnerRestriction(segmentQuery, BusinessEntity.Account, segmentRestriction);
        return segmentRestriction;
    }

    private void setTargetProducts(Restriction rootRestriction, List<String> targetProducts) {
        if (CollectionUtils.isNotEmpty(targetProducts)) {
            String concatenated = StringUtils.join(targetProducts, ",");
            if (rootRestriction instanceof LogicalRestriction) {
                BreadthFirstSearch bfs = new BreadthFirstSearch();
                bfs.run(rootRestriction, (object, ctx) -> {
                    if (object instanceof TransactionRestriction) {
                        TransactionRestriction txRestriction = (TransactionRestriction) object;
                        txRestriction.setTargetProductId(concatenated);
                    }
                });
            } else if (rootRestriction instanceof TransactionRestriction) {
                TransactionRestriction txRestriction = (TransactionRestriction) rootRestriction;
                txRestriction.setTargetProductId(concatenated);
            }
        }
    }

}
