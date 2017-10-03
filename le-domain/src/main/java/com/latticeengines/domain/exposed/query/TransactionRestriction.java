package com.latticeengines.domain.exposed.query;

import java.util.Collections;

import org.apache.commons.lang.RandomStringUtils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

import static com.latticeengines.domain.exposed.query.AggregationType.AT_LEAST_ONCE;
import static com.latticeengines.domain.exposed.query.AggregationType.EACH;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class TransactionRestriction extends Restriction {
    public static String PERIOD_AMOUNT = "PeriodAmount";
    public static String PERIOD_QUANTITY = "PeriodQuantity";

    @JsonProperty("productName")
    private String productName;

    @JsonProperty("productId")
    private String productId;

    @JsonProperty("timeFilter")
    private TimeFilter timeFilter;

    @JsonProperty("negate")
    private boolean negate;

    @JsonProperty("spentFilter")
    private AggregationFilter spentFilter;

    @JsonProperty("unitFilter")
    private AggregationFilter unitFilter;

    public TransactionRestriction() {
    }

    public TransactionRestriction(String productName, String productId, TimeFilter timeFilter, boolean negate,
            AggregationFilter spentFilter, AggregationFilter unitFilter) {
        this.productName = productName;
        this.productId = productId;
        this.timeFilter = timeFilter;
        this.negate = negate;
        this.spentFilter = spentFilter;
        this.unitFilter = unitFilter;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public TimeFilter getTimeFilter() {
        return timeFilter;
    }

    public void setTimeFilter(TimeFilter timeFilter) {
        this.timeFilter = timeFilter;
    }

    public boolean isNegate() {
        return negate;
    }

    public void setNegate(boolean negate) {
        this.negate = negate;
    }

    public Restriction convert(BusinessEntity entity) {
        Restriction productRestriction = filterByProduct();

        Restriction productTimeRestriction = filterByTime(productRestriction);
        AttributeLookup amountLookup = new AttributeLookup(BusinessEntity.Transaction,
                                                           InterfaceName.TotalAmount.name());
        AttributeLookup quantityLookup = new AttributeLookup(BusinessEntity.Transaction,
                                                             InterfaceName.TotalQuantity.name());
        AggregateLookup periodAmount = AggregateLookup.sum(amountLookup).as(PERIOD_AMOUNT);
        AggregateLookup periodQuantity = AggregateLookup.sum(quantityLookup).as(PERIOD_QUANTITY);
        AttributeLookup accountId = new AttributeLookup(BusinessEntity.Transaction, InterfaceName.AccountId.name());

        Query innerQuery = Query.builder().from(BusinessEntity.Transaction)
                .select(accountId, periodAmount, periodQuantity)
                .where(productTimeRestriction)
                .groupBy(groupByAccountAndPeriod(accountId))
                .having(filterByPeriodAmountQuantity(periodAmount, periodQuantity))
                .build();

        SubQuery innerSubQuery = new SubQuery(innerQuery, generateAlias(BusinessEntity.Transaction));

        SubQueryAttrLookup subQueryAccountId = new SubQueryAttrLookup(innerSubQuery, InterfaceName.AccountId.name());
        SubQueryAttrLookup periodTotalAmountLookup = new SubQueryAttrLookup(innerSubQuery, PERIOD_AMOUNT);
        SubQueryAttrLookup periodTotalQuantityLookup = new SubQueryAttrLookup(innerSubQuery, PERIOD_QUANTITY);
        Query outerQuery = Query.builder().from(innerSubQuery)
                .select(subQueryAccountId)
                .groupBy(subQueryAccountId)
                .having(filterByAggregatedPeriodAmountQuantity(periodTotalAmountLookup, periodTotalQuantityLookup)).build();

        SubQuery txSubQuery = new SubQuery(outerQuery, generateAlias(BusinessEntity.Transaction));
        ConcreteRestriction accountInRestriction = (ConcreteRestriction) Restriction.builder()
                .let(entity, InterfaceName.AccountId.name()).inCollection(txSubQuery, InterfaceName.AccountId.name())
                .build();

        return isNegate() ? Restriction.builder().not(accountInRestriction).build() : accountInRestriction;
    }

    private Lookup[] groupByAccountAndPeriod(AttributeLookup accountId) {
        // todo, add period lookup
        return new Lookup[]{accountId};
    }

    private Restriction filterByPeriodAmountQuantity(AggregateLookup aggrAmount, AggregateLookup aggrQuantity) {
        Restriction restriction = null;
        if (spentFilter != null && unitFilter != null) {
            Restriction amountRestriction = filterByAggregationType(aggrAmount, spentFilter);
            Restriction quantityRestriction = filterByAggregationType(aggrQuantity, unitFilter);
            restriction = Restriction.builder().and(amountRestriction, quantityRestriction).build();
        } else if (spentFilter != null) {
            restriction = filterByAggregationType(aggrAmount, spentFilter);
        } else if (unitFilter != null) {
            restriction = filterByAggregationType(aggrQuantity, unitFilter);
        }
        return restriction;
    }

    private Restriction filterByAggregationType(Lookup lookup, AggregationFilter filter) {
        Restriction restriction = null;
        if (EACH == filter.getAggregationType() || AT_LEAST_ONCE == filter.getAggregationType()) {
            restriction = convertValueComparison(lookup, filter.getComparisonType(), filter.getValue());
        }
        return restriction;
    }

    private Restriction filterByAggregatedPeriodAmountQuantity(Lookup periodAmountLookup, Lookup periodQuantityLookup) {

        Restriction restriction;
        if (spentFilter == null && unitFilter == null) {
            // has purchased, treat it as sum(amount) > 0 or sum(unit) > 0
            AggregateLookup aggrAmount = AggregateLookup.sum(periodAmountLookup);
            AggregateLookup aggrQuantity = AggregateLookup.sum(periodQuantityLookup);
            Restriction amountRestriction = Restriction.builder().let(aggrAmount).gt(0).build();
            Restriction quantityRestriction = Restriction.builder().let(aggrQuantity).gt(0).build();
            restriction = Restriction.builder().or(amountRestriction, quantityRestriction).build();
        } else if (spentFilter != null && unitFilter == null) {
            restriction = getAggregatedRestriction(periodAmountLookup, spentFilter);
        } else if (spentFilter == null && unitFilter != null) {
            restriction = getAggregatedRestriction(periodQuantityLookup, unitFilter);
        } else {
            Restriction amountRestriction = getAggregatedRestriction(periodAmountLookup, spentFilter);
            Restriction quantityRestriction = getAggregatedRestriction(periodQuantityLookup, unitFilter);
            restriction = Restriction.builder().and(amountRestriction, quantityRestriction).build();
        }
        return restriction;
    }

    private Restriction getAggregatedRestriction(Lookup mixin, AggregationFilter filter) {
        Restriction restriction = null;
        switch (filter.getAggregationType()) {
        case AVG:
            restriction = convertValueComparison(AggregateLookup.avg(mixin),
                                                 filter.getComparisonType(), filter.getValue());
            break;
        case SUM:
            restriction = convertValueComparison(AggregateLookup.sum(mixin),
                                                 filter.getComparisonType(), filter.getValue());
            break;
        case AT_LEAST_ONCE:
            restriction = Restriction.builder().let(AggregateLookup.count()).gt(0).build();
            break;
        case EACH:
            // todo, calculate time period count
            // restriction = Restriction.builder().let(AggregateLookup.count()).eq(periodCount).build();
        default:
            throw new UnsupportedOperationException(
                    "Unsupported aggregation type " + filter.getAggregationType());
        }
        return restriction;
    }

    private String generateAlias(BusinessEntity entity) {
        return entity.name() + RandomStringUtils.randomAlphanumeric(8);
    }

    private Restriction filterByProduct() {
        return Restriction.builder().let(BusinessEntity.Transaction, InterfaceName.ProductId.name()).eq(getProductId())
                .build();
    }

    private Restriction filterByTime(Restriction restriction) {
        if (timeFilter == null) {
            timeFilter = new TimeFilter(ComparisonType.EVER, null, Collections.emptyList());
        }
        timeFilter.setLhs(new DateAttributeLookup(BusinessEntity.Transaction, InterfaceName.TransactionDate.name(),
                timeFilter.getPeriod()));
        return Restriction.builder().and(restriction, this.getTimeFilter()).build();
    }

}
