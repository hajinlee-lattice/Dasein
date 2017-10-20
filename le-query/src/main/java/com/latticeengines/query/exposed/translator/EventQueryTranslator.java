package com.latticeengines.query.exposed.translator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.common.exposed.graph.traversal.impl.BreadthFirstSearch;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.QueryBuilder;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.SelectAllLookup;
import com.latticeengines.domain.exposed.query.SubQuery;
import com.latticeengines.domain.exposed.query.TransactionRestriction;
import com.latticeengines.domain.exposed.query.UnionLookup;
import com.latticeengines.domain.exposed.util.RestrictionUtils;

public class EventQueryTranslator {
    public static Query translate(Restriction restriction) {
        return translateRestriction(translateFrontendRestriction(restriction));
    }

    private static SubQuery translateTransactionRestriction(TransactionRestriction txRestriction) {
        // todo
        throw new UnsupportedOperationException("not implemented yet");
    }

    private static SubQuery translateConcreteRestriction(ConcreteRestriction restriction) {
        // todo
        throw new UnsupportedOperationException("not implemented yet");
    }

    private static Query translateRestriction(Restriction restriction) {
        QueryBuilder builder = Query.builder();

        // visit leaf nodes and build sub queries
        if (restriction instanceof LogicalRestriction) {
            UnionLookup unionLookup = new UnionLookup();
            Map<LogicalRestriction, List<Lookup>> restrictionMap = new HashMap<>();

            BreadthFirstSearch bfs = new BreadthFirstSearch();
            bfs.run(restriction, (object, ctx) -> {
                if (object instanceof LogicalRestriction) {
                    LogicalRestriction logicalRestriction = (LogicalRestriction) object;
                    restrictionMap.put(logicalRestriction, new ArrayList<>());
                    LogicalRestriction parent = (LogicalRestriction) ctx.getProperty("parent");
                    // root
                    if (parent == null) {
                        unionLookup.setRootRestriction(logicalRestriction);
                    }
                } else if (object instanceof TransactionRestriction) {
                    TransactionRestriction txRestriction = (TransactionRestriction) restriction;
                    SubQuery subQuery = translateTransactionRestriction(txRestriction);
                    LogicalRestriction parent = (LogicalRestriction) ctx.getProperty("parent");
                    List<Lookup> parentDecorator = restrictionMap.get(parent);
                    parentDecorator.add(new SelectAllLookup(subQuery.getAlias()));
                    builder.with(subQuery);
                } else if (object instanceof ConcreteRestriction) {
                    ConcreteRestriction concreteRestriction = (ConcreteRestriction) object;
                    SubQuery subQuery = translateConcreteRestriction(concreteRestriction);
                    LogicalRestriction parent = (LogicalRestriction) ctx.getProperty("parent");
                    List<Lookup> parentDecorator = restrictionMap.get(parent);
                    parentDecorator.add(new SelectAllLookup(subQuery.getAlias()));
                    builder.with(subQuery);
                }
            });
            unionLookup.setLookupMap(restrictionMap);
            builder.from(unionLookup);
        } else if (restriction instanceof TransactionRestriction) {
            SubQuery subQuery = translateTransactionRestriction((TransactionRestriction) restriction);
            builder.from(subQuery);
        } else if (restriction instanceof ConcreteRestriction) {
            SubQuery subQuery = translateConcreteRestriction((ConcreteRestriction) restriction);
            builder.from(subQuery);
        } else {
            throw new UnsupportedOperationException("Cannot translate restriction " + restriction);
        }

        return builder.build();
    }

    // translate BucketRestriction
    private static Restriction translateFrontendRestriction(Restriction restriction) {
        Restriction translated;
        if (restriction instanceof LogicalRestriction) {
            BreadthFirstSearch search = new BreadthFirstSearch();
            search.run(restriction, (object, ctx) -> {
                if (object instanceof BucketRestriction) {
                    BucketRestriction bucket = (BucketRestriction) object;
                    Restriction converted = RestrictionUtils.convertBucketRestriction(bucket);
                    LogicalRestriction parent = (LogicalRestriction) ctx.getProperty("parent");
                    parent.getRestrictions().remove(bucket);
                    parent.getRestrictions().add(converted);
                }
            });
            translated = restriction;
        } else if (restriction instanceof BucketRestriction) {
            BucketRestriction bucket = (BucketRestriction) restriction;
            translated = RestrictionUtils.convertBucketRestriction(bucket);
        } else {
            translated = restriction;
        }

        return translated;
    }
}
