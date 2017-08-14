package com.latticeengines.domain.exposed.util;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.LogicalOperator;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Restriction;

public class RestrictionOptimizer {

    public static Restriction optimize(Restriction restriction) {
        if (restriction == null || restriction instanceof BucketRestriction
                || restriction instanceof ConcreteRestriction) {
            return restriction;
        } else if (restriction instanceof LogicalRestriction) {
            return optimizeLogicalRestriction((LogicalRestriction) restriction);
        } else {
            throw new RuntimeException("Can only optimize logical, bucket and concrete restrictions");
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
                if (restriction instanceof LogicalRestriction) {
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

    public static Restriction group(Restriction restriction) {
        if (restriction == null || restriction instanceof BucketRestriction) {
            return restriction;
        } else if (restriction instanceof LogicalRestriction) {
            return groupLogicalRestriction((LogicalRestriction) restriction);
        } else {
            throw new RuntimeException("Can only group bucket and logical restrictions");
        }
    }

    private static Restriction groupLogicalRestriction(LogicalRestriction logicalRestriction) {
        List<Restriction> nonLogicals = new ArrayList<>();
        List<Restriction> logicals = new ArrayList<>();
        logicalRestriction.getRestrictions().forEach(restriction -> {
            if (restriction instanceof LogicalRestriction) {
                logicals.add(groupLogicalRestriction((LogicalRestriction) restriction));
            } else {
                nonLogicals.add(restriction);
            }
        });

        Map<BusinessEntity, List<Restriction>> restrictionMap = new HashMap<>();
        for (Restriction restriction : nonLogicals) {
            BusinessEntity entity;
            if (restriction instanceof BucketRestriction) {
                entity = ((BucketRestriction) restriction).getAttr().getEntity();
            } else if (restriction instanceof ConcreteRestriction) {
                ConcreteRestriction concreteRestriction = (ConcreteRestriction) restriction;
                if (concreteRestriction.getLhs() instanceof AttributeLookup) {
                    entity = ((AttributeLookup) concreteRestriction.getLhs()).getEntity();
                } else {
                    throw new UnsupportedOperationException(
                            "LHS of concretion restriction is not an attribute lookup.");
                }
            } else {
                throw new UnsupportedOperationException("Only support logical, bucket and concrete restrictions.");
            }
            if (!restrictionMap.containsKey(entity)) {
                restrictionMap.put(entity, new ArrayList<>());
            }
            restrictionMap.get(entity).add(restriction);
        }
        restrictionMap.entrySet().stream().sorted(Comparator.comparing(Map.Entry::getKey)) //
                .map(Map.Entry::getValue).forEach(restrictions -> {
                    if (restrictions.size() == 1) {
                        logicals.add(restrictions.get(0));
                    } else if (logicalRestriction.getOperator().equals(LogicalOperator.OR)) {
                        logicals.add(Restriction.builder().or(restrictions).build());
                    } else {
                        logicals.add(Restriction.builder().and(restrictions).build());
                    }
                });
        logicalRestriction.setRestrictions(logicals);
        return logicalRestriction;
    }

}
