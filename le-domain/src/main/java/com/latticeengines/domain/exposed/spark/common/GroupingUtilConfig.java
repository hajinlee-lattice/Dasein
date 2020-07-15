package com.latticeengines.domain.exposed.spark.common;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.cdl.activity.JourneyStage;
import com.latticeengines.domain.exposed.cdl.activity.JourneyStagePredicate;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.AggregateLookup;
import com.latticeengines.domain.exposed.query.CollectionLookup;
import com.latticeengines.domain.exposed.query.ColumnLookup;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.LogicalOperator;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.ValueLookup;
import com.latticeengines.domain.exposed.util.TimeLineStoreUtils;

public class GroupingUtilConfig {
    private String groupKey;

    /**
     * Intended to a list of restrictions that will be OR'd in spark group query
     * This is the initial design Final design should have this as a restriction,
     * that is converted into a query by the query layer
     */
    private Restriction filterRestrictions;
    private AggregateLookup aggregateLookup;

    public String getGroupKey() {
        return groupKey;
    }

    public void setGroupKey(String groupKey) {
        this.groupKey = groupKey;
    }

    public Restriction getFilterRestrictions() {
        return filterRestrictions;
    }

    public void setFilterRestrictions(Restriction filterRestrictions) {
        this.filterRestrictions = filterRestrictions;
    }

    public AggregateLookup getAggregateLookup() {
        return aggregateLookup;
    }

    public void setAggregateLookup(AggregateLookup aggregateLookup) {
        this.aggregateLookup = aggregateLookup;
    }

    public static GroupingUtilConfig from(JourneyStage stage, JourneyStagePredicate predicate, Instant now) {
        GroupingUtilConfig config = new GroupingUtilConfig();
        config.setGroupKey(InterfaceName.AccountId.name());

        AggregateLookup aggregateLookup = AggregateLookup.count();
        aggregateLookup.setLookup(new ColumnLookup(InterfaceName.AccountId.name()));
        aggregateLookup.setAlias(AggregateLookup.Aggregator.COUNT.name());
        config.setAggregateLookup(aggregateLookup);

        List<Restriction> filterRestrictions = new ArrayList<>();

        // Stream Restriction
        ConcreteRestriction streamTypeFilter = new ConcreteRestriction(false, //
                new ColumnLookup(InterfaceName.StreamType.name()), //
                ComparisonType.EQUAL, //
                new ValueLookup(predicate.getStreamType().name()));
        filterRestrictions.add(streamTypeFilter);

        // Timestamp filter
        ConcreteRestriction timeStampFilter = new ConcreteRestriction(false, //
                new ColumnLookup(InterfaceName.EventTimestamp.name()), //
                ComparisonType.GREATER_THAN, //
                new ValueLookup(TimeLineStoreUtils.toEventTimestampNDaysAgo(now, predicate.getPeriodDays())));
        filterRestrictions.add(timeStampFilter);

        // Contact not null
        if (predicate.isContactNotNull())
            filterRestrictions.add(new ConcreteRestriction(false, //
                    new ColumnLookup(InterfaceName.ContactId.name()), //
                    ComparisonType.IS_NOT_NULL, //
                    null));

        // Other StreamField Filters
        predicate.getStreamFieldsToFilter().forEach(stf -> filterRestrictions.add(new ConcreteRestriction(false, //
                new ColumnLookup(stf.getColumnName().name()), //
                stf.getComparisonType(), //
                CollectionUtils.isNotEmpty(stf.getColumnValues()) //
                        ? new CollectionLookup(new ArrayList<>(stf.getColumnValues())) //
                        : (StringUtils.isNotBlank(stf.getColumnValue()) ? new ValueLookup(stf.getColumnValue())
                                : null)))

        );
        config.setFilterRestrictions(new LogicalRestriction(LogicalOperator.AND, filterRestrictions));
        return config;
    }

    public String getAggregationColumn() {
        return ((ColumnLookup) aggregateLookup.getLookup()).getColumnName();
    }

    // Hack! Eventually this should use the Query layer to generate spark queries
    // using custom AttrRepo
    public String getSparkSqlWhereClause() {
        if (!(filterRestrictions instanceof LogicalRestriction))
            throw new UnsupportedOperationException("Unsupported grouping configuration");

        LogicalRestriction lrest = (LogicalRestriction) filterRestrictions;
        StringBuilder clause = new StringBuilder();
        String spacer = " ";
        List<String> list1 = new ArrayList<>();
        for (Restriction rest : lrest.getRestrictions()) {
            ConcreteRestriction cRest = (ConcreteRestriction) rest;
            list1.add(processLhs(cRest.getLhs()) + spacer + processOperator((cRest).getRelation()) + spacer
                    + processRhs(cRest.getRhs()));

        }
        return CollectionUtils.isEmpty(list1) ? ""
                : String.join(lrest.getOperator() == LogicalOperator.AND ? " and " : " or ", list1);
    }

    private String processRhs(Lookup rhs) {
        if (rhs == null)
            return "";
        if (rhs instanceof ValueLookup)
            return "'" + ((ValueLookup) rhs).getValue().toString() + "'";
        if (rhs instanceof CollectionLookup)
            return "(" + String.join(",",
                    ((CollectionLookup) rhs).getValues().stream().map(v -> "'" + v + "'").collect(Collectors.toList()))
                    + ")";
        return "";
    }

    private String processLhs(Lookup lhs) {
        return ((ColumnLookup) lhs).getColumnName();
    }

    private String processOperator(ComparisonType comparisonType) {
        switch (comparisonType) {
        case EQUAL:
            return "=";
        case IS_NULL:
            return "is null";
        case IS_NOT_NULL:
            return "is not null";
        case GREATER_THAN:
            return ">";
        case IN_COLLECTION:
            return "in";
        case CONTAINS:
            return "like";
        case NOT_CONTAINS:
            return "not like";
        default:
            throw new UnsupportedOperationException(comparisonType.name() + "is not supported");
        }
    }
}
