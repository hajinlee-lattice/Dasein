package com.latticeengines.domain.exposed.spark.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.JourneyStage;
import com.latticeengines.domain.exposed.cdl.activity.JourneyStagePredicate;
import com.latticeengines.domain.exposed.cdl.activity.StreamFieldToFilter;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.ComparisonType;

public class GroupingConfgUnitTestNG {

    @Test(groups = "unit")
    public void testJourneyStageSparkFilterClauses() {
        JourneyStage stage = new JourneyStage();
        List<JourneyStagePredicate> predicates;
        JourneyStagePredicate predicate;
        StreamFieldToFilter streamFieldToFilter;
        GroupingUtilConfig config;

        // Test 1: Known Engaged-> MarketingActivity 1 14 Days EventType in list
        // ["WebVisit"]
        stage.setStageName("Known Engaged");
        predicates = new ArrayList<>();

        predicate = new JourneyStagePredicate();
        predicate.setStreamType(AtlasStream.StreamType.MarketingActivity);
        predicate.setPeriodDays(14);
        predicate.setNoOfEvents(1);

        streamFieldToFilter = new StreamFieldToFilter();
        streamFieldToFilter.setColumnName(InterfaceName.Detail1);
        streamFieldToFilter.setColumnValues(Arrays.asList("WebVisit", "Email Bounce"));
        streamFieldToFilter.setComparisonType(ComparisonType.IN_COLLECTION);

        predicate.setStreamFieldsToFilter(Collections.singletonList(streamFieldToFilter));
        predicates.add(predicate);
        stage.setPredicates(predicates);

        config = GroupingUtilConfig.from(stage, predicate);
        Assert.assertNotNull(config);
        Assert.assertEquals(config.getGroupKey(), InterfaceName.AccountId.name());
        Assert.assertNotNull(config.getAggregateLookup());
        Assert.assertEquals(config.getAggregationColumn(), InterfaceName.AccountId.name());
        Assert.assertEquals(config.getSparkSqlWhereClause(),
                "StreamType = 'MarketingActivity' and EventTimestamp > '1593302400' and Detail1 in ('WebVisit','Email Bounce')");

        // Test 2: Opportunity-> Opportunity 1 90 Days Detail1 NOT CONTAINS "Closed%"
        stage.setStageName("Opportunity");
        predicates = new ArrayList<>();

        predicate = new JourneyStagePredicate();
        predicate.setStreamType(AtlasStream.StreamType.Opportunity);
        predicate.setPeriodDays(90);
        predicate.setNoOfEvents(1);

        streamFieldToFilter = new StreamFieldToFilter();
        streamFieldToFilter.setColumnName(InterfaceName.Detail1);
        streamFieldToFilter.setColumnValue("closed%");
        streamFieldToFilter.setComparisonType(ComparisonType.CONTAINS);
        predicate.setStreamFieldsToFilter(new ArrayList<>(Collections.singletonList(streamFieldToFilter)));

        streamFieldToFilter = new StreamFieldToFilter();
        streamFieldToFilter.setColumnName(InterfaceName.Detail1);
        streamFieldToFilter.setColumnValue("%won");
        streamFieldToFilter.setComparisonType(ComparisonType.CONTAINS);
        predicate.getStreamFieldsToFilter().add(streamFieldToFilter);
        predicates.add(predicate);
        stage.setPredicates(predicates);

        config = GroupingUtilConfig.from(stage, predicate);
        Assert.assertNotNull(config);
        Assert.assertEquals(config.getGroupKey(), InterfaceName.AccountId.name());
        Assert.assertNotNull(config.getAggregateLookup());
        Assert.assertEquals(config.getAggregationColumn(), InterfaceName.AccountId.name());
        Assert.assertEquals(config.getSparkSqlWhereClause(),
                "StreamType = 'Opportunity' and EventTimestamp > '1586736000' and Detail1 like 'closed%' and Detail1 like '%won'");

    }
}
