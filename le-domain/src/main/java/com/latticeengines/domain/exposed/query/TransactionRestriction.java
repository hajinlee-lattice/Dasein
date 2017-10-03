package com.latticeengines.domain.exposed.query;

import java.util.Collections;

import org.apache.commons.lang.RandomStringUtils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class TransactionRestriction extends Restriction {

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

        AttributeLookup accountId = new AttributeLookup(BusinessEntity.Transaction, InterfaceName.AccountId.name());
        Query innerQuery = Query.builder().from(BusinessEntity.Transaction).select(accountId)
                .where(productTimeRestriction).groupBy(accountId).having(filterByAmountQuantity()).build();

        SubQuery txSubQuery = new SubQuery(innerQuery, generateAlias(BusinessEntity.Transaction));
        ConcreteRestriction accountInRestriction = (ConcreteRestriction) Restriction.builder()
                .let(entity, InterfaceName.AccountId.name()).inCollection(txSubQuery, InterfaceName.AccountId.name())
                .build();

        return isNegate() ? Restriction.builder().not(accountInRestriction).build() : accountInRestriction;
    }

    private Restriction filterByAmountQuantity() {
        AttributeLookup amountLookup = new AttributeLookup(BusinessEntity.Transaction,
                InterfaceName.TotalAmount.name());
        AttributeLookup quantityLookup = new AttributeLookup(BusinessEntity.Transaction,
                InterfaceName.TotalAmount.name());
        Restriction restriction;
        if (spentFilter == null && unitFilter == null) {
            // has purchased, treat it as sum(amount) > 0 or sum(unit) > 0
            AggregateLookup aggrAmount = AggregateLookup.sum(amountLookup);
            AggregateLookup aggrQuantity = AggregateLookup.sum(quantityLookup);
            Restriction amountRestriction = Restriction.builder().let(aggrAmount).gt(0).build();
            Restriction quantityRestriction = Restriction.builder().let(aggrQuantity).gt(0).build();
            restriction = Restriction.builder().or(amountRestriction, quantityRestriction).build();
        } else if (spentFilter != null && unitFilter == null) {
            AggregateLookup aggrAmount = getAggregateLookup(amountLookup, spentFilter);
            restriction = convertValueComparison(aggrAmount, spentFilter.getComparisonType(), spentFilter.getValue());
        } else if (spentFilter == null && unitFilter != null) {
            AggregateLookup aggrQuantity = getAggregateLookup(quantityLookup, unitFilter);
            restriction = convertValueComparison(aggrQuantity, unitFilter.getComparisonType(), unitFilter.getValue());
        } else {
            AggregateLookup aggrAmount = getAggregateLookup(amountLookup, spentFilter);
            Restriction amountRestriction = convertValueComparison(aggrAmount, spentFilter.getComparisonType(),
                    spentFilter.getValue());
            AggregateLookup aggrQuantity = getAggregateLookup(quantityLookup, unitFilter);
            Restriction quantityRestriction = convertValueComparison(aggrQuantity, unitFilter.getComparisonType(),
                    unitFilter.getValue());
            restriction = Restriction.builder().and(amountRestriction, quantityRestriction).build();
        }
        return restriction;
    }

    private AggregateLookup getAggregateLookup(Lookup mixin, AggregationFilter aggregationFilter) {
        switch (aggregationFilter.getAggregationType()) {
        case AVG:
            return AggregateLookup.avg(mixin);
        case SUM:
            return AggregateLookup.sum(mixin);
        default:
            throw new UnsupportedOperationException(
                    "Unsupported aggregation type " + aggregationFilter.getAggregationType());
        }
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
