package com.latticeengines.domain.exposed.util;

import java.util.ArrayList;
import java.util.List;

import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.TransactionRestriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;

public class RestrictionOptimizer {

    public static void optimize(FrontEndQuery frontEndQuery) {
        if (frontEndQuery.getAccountRestriction() != null) {
            Restriction restriction = frontEndQuery.getAccountRestriction().getRestriction();
            if (restriction != null) {
                frontEndQuery.getAccountRestriction().setRestriction(RestrictionOptimizer.optimize(restriction));
            }
        }
        if (frontEndQuery.getContactRestriction() != null) {
            Restriction restriction = frontEndQuery.getContactRestriction().getRestriction();
            if (restriction != null) {
                frontEndQuery.getContactRestriction().setRestriction(RestrictionOptimizer.optimize(restriction));
            }
        }
    }

    public static Restriction optimize(Restriction restriction) {
        if (restriction == null) {
            return null;
        } else if (restriction instanceof ConcreteRestriction || restriction instanceof TransactionRestriction) {
            return restriction;
        } else if (restriction instanceof BucketRestriction) {
            return optimizeBucketRestriction((BucketRestriction) restriction);
        } else if (restriction instanceof LogicalRestriction) {
            return optimizeLogicalRestriction((LogicalRestriction) restriction);
        } else {
            throw new RuntimeException("Cannot optimize restriction of type " + restriction.getClass());
        }
    }

    private static BucketRestriction optimizeBucketRestriction(BucketRestriction bucket) {
        bucket.setSelected(null);
        if (Boolean.TRUE.equals(bucket.getIgnored())) {
            return null;
        } else {
            bucket.setIgnored(null);
            return bucket;
        }
    }

    private static Restriction optimizeLogicalRestriction(LogicalRestriction logicalRestriction) {
        if (logicalRestriction.getRestrictions() == null || logicalRestriction.getRestrictions().isEmpty()) {
            return null;
        }

        List<Restriction> children = new ArrayList<>();
        logicalRestriction.getRestrictions().forEach(restriction -> {
            if (restriction != null) {
                Restriction flatRestriction;
                if (restriction instanceof LogicalRestriction || restriction instanceof BucketRestriction) {
                    flatRestriction = optimize(restriction);
                } else {
                    flatRestriction = restriction;
                }
                if (flatRestriction != null) {
                    if (flatRestriction instanceof LogicalRestriction) {
                        LogicalRestriction flattenLogic = (LogicalRestriction) flatRestriction;
                        if (flattenLogic.getOperator().equals(logicalRestriction.getOperator())) {
                            children.addAll(((LogicalRestriction) flatRestriction).getRestrictions());
                        } else {
                            children.add(flattenLogic);
                        }
                    } else {
                        children.add(flatRestriction);
                    }
                }
            }
        });

        logicalRestriction.setRestrictions(children);

        if (children.isEmpty()) {
            return null;
        } else if (logicalRestriction.getRestrictions().size() == 1) {
            return logicalRestriction.getRestrictions().get(0);
        } else {
            return logicalRestriction;
        }
    }

}
