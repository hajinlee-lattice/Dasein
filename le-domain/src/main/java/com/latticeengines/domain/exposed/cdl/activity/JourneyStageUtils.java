package com.latticeengines.domain.exposed.cdl.activity;

import static com.latticeengines.domain.exposed.cdl.activity.AtlasStream.StreamType.DnbIntentData;
import static com.latticeengines.domain.exposed.cdl.activity.AtlasStream.StreamType.MarketingActivity;
import static com.latticeengines.domain.exposed.cdl.activity.AtlasStream.StreamType.Opportunity;
import static com.latticeengines.domain.exposed.cdl.activity.AtlasStream.StreamType.WebVisit;

import java.util.Arrays;
import java.util.List;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.security.Tenant;

public final class JourneyStageUtils {
    private static final int DEFAULT_PERIOD = 90;

    private JourneyStageUtils() {
    }

    /**
     * Default set of journey stages for all tenants
     *
     * @param tenant
     *            target tenant to create journey stages for
     * @return list of journey stages, sorted by priority
     */
    public static List<JourneyStage> atlasJourneyStages(@NotNull Tenant tenant) {
        return Arrays.asList( //
                closedWonStage(tenant), //
                closedStage(tenant), //
                opportunityStage(tenant), //
                contactInquiryStage(tenant), //
                knownEngagedStage(tenant), //
                engagedStage(tenant), //
                awareStage(tenant), //
                darkStage(tenant) //
        );
    }

    private static JourneyStage closedWonStage(@NotNull Tenant tenant) {
        return stage(tenant, "Closed-Won", "Opportunity is closed-won", 8, "#3FA40F",
                predicate(Opportunity, 1, closedFilter(), wonFilter()));
    }

    private static JourneyStage closedStage(@NotNull Tenant tenant) {
        return stage(tenant, "Closed", "Opportunity is closed", 7, "#70BF4A",
                predicate(Opportunity, 1, closedFilter()));
    }

    private static JourneyStage opportunityStage(@NotNull Tenant tenant) {
        return stage(tenant, "Opportunity", "Opportunity on Account", 6, "#33BDB7",
                predicate(Opportunity, 1, notClosedFilter()));
    }

    private static JourneyStage contactInquiryStage(@NotNull Tenant tenant) {
        return stage(tenant, "Contact Inquiry", "Contact has taken action", 5, "#8E71B2",
                predicate(MarketingActivity, 1, 90, formFillFilter()));
    }

    private static JourneyStage knownEngagedStage(@NotNull Tenant tenant) {
        return stage(tenant, "Known Engaged", "Account is being engaged with", 4, "#2A5587",
                predicate(MarketingActivity, 1, 14, webVisitFilter()));
    }

    private static JourneyStage engagedStage(@NotNull Tenant tenant) {
        return stage(tenant, "Engaged", "Account has website visits", 3, "#457EBA", predicate(WebVisit, 1, 14));
    }

    private static JourneyStage awareStage(@NotNull Tenant tenant) {
        return stage(tenant, "Aware", "Account is showing interest", 2, "#51C1ED", predicate(DnbIntentData, 1, 28));
    }

    private static JourneyStage darkStage(@NotNull Tenant tenant) {
        // default, can have empty predicate
        return stage(tenant, "Dark", "No Activity", 1, "#BCC9DB", new JourneyStagePredicate());
    }

    private static JourneyStage stage(@NotNull Tenant tenant, @NotNull String stageName, String description,
            int priority, String colorCode, JourneyStagePredicate... predicates) {
        return new JourneyStage.Builder() //
                .withTenant(tenant) //
                .withStageName(stageName) //
                .withDisplayName(stageName) //
                .withDescription(description) //
                .withPriority(priority) //
                .withDisplayColorCode(colorCode) //
                .withPredicates(Arrays.asList(predicates)) //
                .build();
    }

    private static JourneyStagePredicate predicate(@NotNull AtlasStream.StreamType type, int noOfEvents,
            StreamFieldToFilter... filters) {
        return predicate(type, noOfEvents, DEFAULT_PERIOD, filters);
    }

    private static JourneyStagePredicate predicate(@NotNull AtlasStream.StreamType type, int noOfEvents, int period,
            StreamFieldToFilter... filters) {
        JourneyStagePredicate predicate = new JourneyStagePredicate();
        predicate.setPeriodDays(period);
        predicate.setNoOfEvents(noOfEvents);
        predicate.setStreamType(type);
        predicate.setStreamFieldsToFilter(Arrays.asList(filters));
        return predicate;
    }

    private static StreamFieldToFilter webVisitFilter() {
        StreamFieldToFilter filter = new StreamFieldToFilter();
        filter.setComparisonType(ComparisonType.EQUAL);
        filter.setColumnName(InterfaceName.EventType);
        filter.setColumnValue("WebVisit");
        return filter;
    }

    private static StreamFieldToFilter formFillFilter() {
        StreamFieldToFilter filter = new StreamFieldToFilter();
        filter.setComparisonType(ComparisonType.IN_COLLECTION);
        filter.setColumnName(InterfaceName.EventType);
        filter.setColumnValues(Arrays.asList("Fill Out Form", "FormSubmit"));
        return filter;
    }

    private static StreamFieldToFilter notClosedFilter() {
        StreamFieldToFilter filter = new StreamFieldToFilter();
        filter.setComparisonType(ComparisonType.NOT_CONTAINS);
        filter.setColumnName(InterfaceName.Detail1);
        filter.setColumnValue("Closed%");
        return filter;
    }

    private static StreamFieldToFilter closedFilter() {
        StreamFieldToFilter filter = new StreamFieldToFilter();
        filter.setComparisonType(ComparisonType.CONTAINS);
        filter.setColumnName(InterfaceName.Detail1);
        filter.setColumnValue("Closed%");
        return filter;
    }

    private static StreamFieldToFilter wonFilter() {
        StreamFieldToFilter filter = new StreamFieldToFilter();
        filter.setComparisonType(ComparisonType.CONTAINS);
        filter.setColumnName(InterfaceName.Detail1);
        filter.setColumnValue("%Won");
        return filter;
    }
}
