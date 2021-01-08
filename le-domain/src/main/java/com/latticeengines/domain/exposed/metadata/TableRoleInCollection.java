package com.latticeengines.domain.exposed.metadata;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

public enum TableRoleInCollection {
    ConsolidatedAccount, //
    ConsolidatedContact, //
    ConsolidatedProduct, //
    ConsolidatedRawTransaction, //
    ConsolidatedDailyTransaction, //
    ConsolidatedPeriodTransaction, //
    ConsolidatedCatalog, //
    ConsolidatedActivityStream, //

    RawTransactionStream, //
    DailyTransactionStream, //
    PeriodTransactionStream, //
    SpendingAnalysisPeriod, //

    SystemAccount, //
    SystemContact, //
    LatticeAccount, //

    PivotedRating, //

    Profile, //
    AccountProfile, //
    LatticeAccountProfile, //
    ContactProfile, //
    PurchaseHistoryProfile, //

    BucketedAccount, //
    SortedContact, //
    SortedProduct, //
    SortedProductHierarchy, //

    AggregatedTransaction, //
    AggregatedPeriodTransaction, //
    CalculatedPurchaseHistory, //
    CalculatedDepivotedPurchaseHistory, //
    AggregatedActivityStream, //
    AggregatedActivityStreamDelta, //

    // Curated Account Attribute
    // has only requires one table for serving for now.
    CalculatedCuratedAccountAttribute, //
    CalculatedCuratedContact, //

    AnalyticPurchaseState, //

    AccountFeatures, //
    AccountExport, //
    AccountLookup, //

    WebVisitProfile, //
    OpportunityProfile, //
    AccountMarketingActivityProfile, //
    ContactMarketingActivityProfile, //
    CustomIntentProfile,
    PeriodStores, //
    MetricsGroup, //
    TimelineProfile, //
    AccountJourneyStage, //
    ActivityAlert, //
    AccountMaster, //
    ConsolidatedWebVisit;

    static {
        ConsolidatedAccount.primaryKey = InterfaceName.AccountId;
        ConsolidatedAccount.partitionKey = InterfaceName.AccountId;

        BucketedAccount.primaryKey = InterfaceName.AccountId;
        BucketedAccount.distKey = InterfaceName.AccountId;

        ConsolidatedContact.primaryKey = InterfaceName.ContactId;
        ConsolidatedContact.sortKeys = ImmutableList.of(InterfaceName.ContactId); //FIXME (YSong): I think this is not needed
        ConsolidatedContact.partitionKey = InterfaceName.AccountId;
        ConsolidatedContact.rangeKey = InterfaceName.ContactId;

        CalculatedCuratedContact.primaryKey = InterfaceName.ContactId;
        CalculatedCuratedContact.partitionKey = InterfaceName.AccountId;
        CalculatedCuratedContact.rangeKey = InterfaceName.ContactId;
        CalculatedCuratedContact.distKey = InterfaceName.ContactId; // join to contact

        SortedContact.primaryKey = ConsolidatedContact.primaryKey;
        SortedContact.distKey = BucketedAccount.distKey;
        SortedContact.sortKeys = ImmutableList.of(InterfaceName.ContactId);

        ConsolidatedDailyTransaction.primaryKey = InterfaceName.__Composite_Key__;
        ConsolidatedPeriodTransaction.primaryKey = InterfaceName.__Composite_Key__;

        ConsolidatedCatalog.primaryKey = InterfaceName.InternalId;
        ConsolidatedCatalog.hasSignature = true;
        ConsolidatedActivityStream.primaryKey = InterfaceName.InternalId;
        ConsolidatedActivityStream.hasSignature = true;
        AggregatedActivityStream.primaryKey = InterfaceName.__Composite_Key__;
        AggregatedActivityStream.hasSignature = true;

        RawTransactionStream.partitionKey = InterfaceName.StreamDateId;
        DailyTransactionStream.partitionKey = InterfaceName.StreamDateId;
        PeriodTransactionStream.partitionKey = InterfaceName.PeriodId;
        AggregatedTransaction.primaryKey = InterfaceName.__Composite_Key__;
        AggregatedTransaction.distKey = BucketedAccount.distKey;
        AggregatedTransaction.sortKeys = ImmutableList.of(InterfaceName.TransactionDate);

        AggregatedPeriodTransaction.primaryKey = InterfaceName.__Composite_Key__;
        AggregatedPeriodTransaction.distKey = BucketedAccount.distKey;
        AggregatedPeriodTransaction.sortKeys = //
                ImmutableList.of(InterfaceName.PeriodName, InterfaceName.ProductId, InterfaceName.PeriodId);

        ConsolidatedProduct.primaryKey = InterfaceName.ProductId;
        SortedProduct.primaryKey = InterfaceName.ProductId;
        SortedProduct.distKey = InterfaceName.ProductId;
        SortedProductHierarchy.primaryKey = InterfaceName.ProductId;
        SortedProductHierarchy.distKey = InterfaceName.ProductId;

        CalculatedPurchaseHistory.primaryKey = InterfaceName.AccountId;
        CalculatedPurchaseHistory.partitionKey = InterfaceName.AccountId;

        CalculatedDepivotedPurchaseHistory.primaryKey = InterfaceName.__Composite_Key__;
        CalculatedDepivotedPurchaseHistory.distKey = InterfaceName.AccountId;
        CalculatedDepivotedPurchaseHistory.sortKeys = ImmutableList.of(InterfaceName.ProductId);

        CalculatedCuratedAccountAttribute.primaryKey = InterfaceName.AccountId;
        CalculatedCuratedAccountAttribute.distKey = InterfaceName.AccountId;
        CalculatedCuratedAccountAttribute.partitionKey = InterfaceName.AccountId;

        PivotedRating.primaryKey = InterfaceName.AccountId;
        PivotedRating.distKey = InterfaceName.AccountId;
        PivotedRating.partitionKey = InterfaceName.AccountId;

        AccountFeatures.primaryKey = ConsolidatedAccount.primaryKey;
        AccountExport.primaryKey = ConsolidatedAccount.primaryKey;

        AccountLookup.primaryKey = InterfaceName.AtlasLookupKey;
        AccountLookup.partitionKey = InterfaceName.AtlasLookupKey;

        AccountMaster.primaryKey = InterfaceName.LatticeAccountId;

        WebVisitProfile.primaryKey = InterfaceName.AccountId;
        WebVisitProfile.distKey = InterfaceName.AccountId;
        WebVisitProfile.partitionKey = InterfaceName.AccountId;
        WebVisitProfile.hasSignature = true;

        OpportunityProfile.primaryKey = InterfaceName.AccountId;
        OpportunityProfile.distKey = InterfaceName.AccountId;
        OpportunityProfile.partitionKey = InterfaceName.AccountId;
        OpportunityProfile.hasSignature = true;

        ContactMarketingActivityProfile.primaryKey = InterfaceName.ContactId;
        ContactMarketingActivityProfile.distKey = InterfaceName.ContactId;
        ContactMarketingActivityProfile.hasSignature = true;

        AccountMarketingActivityProfile.primaryKey = InterfaceName.AccountId;
        AccountMarketingActivityProfile.partitionKey = InterfaceName.AccountId;
        AccountMarketingActivityProfile.distKey = InterfaceName.AccountId;
        AccountMarketingActivityProfile.hasSignature = true;

        CustomIntentProfile.primaryKey = InterfaceName.AccountId;
        CustomIntentProfile.partitionKey = InterfaceName.AccountId;
        CustomIntentProfile.distKey = InterfaceName.AccountId;
        CustomIntentProfile.hasSignature = true;

        TimelineProfile.partitionKey = InterfaceName.PartitionKey;
        TimelineProfile.rangeKey = InterfaceName.SortKey;

        AccountJourneyStage.partitionKey = InterfaceName.AccountId;

        ActivityAlert.partitionKey = InterfaceName.AccountId;

        PeriodStores.hasSignature = true;
        MetricsGroup.hasSignature = true;
        TimelineProfile.hasSignature = true;
        ConsolidateWebVisit.hasSignature = true;
    }

    private static final Logger log = LoggerFactory.getLogger(TableRoleInCollection.class);

    private InterfaceName primaryKey;
    private boolean hasSignature; // whether table of this role has signature

    // for redshift tables
    private InterfaceName distKey;
    private ImmutableList<InterfaceName> sortKeys = ImmutableList.<InterfaceName> builder().build();

    // for dynamo tables
    private InterfaceName partitionKey;
    private InterfaceName rangeKey;

    public String getPrimaryKey() {
        if (primaryKey == null) {
            log.warn(this.name() + " does not have a primary key.");
            return null;
        } else {
            return primaryKey.name();
        }
    }

    public String getDistKey() {
        if (distKey == null) {
            log.warn(this.name() + " does not have a dist key.");
            return null;
        } else {
            return distKey.name();
        }
    }

    public ImmutableList<String> getSortKeys() {
        List<String> sortKeyNames = new ArrayList<>();
        sortKeyNames.add(getDistKey());
        sortKeyNames.addAll(this.sortKeys.stream().map(InterfaceName::name).collect(Collectors.toList()));
        return ImmutableList.copyOf(sortKeyNames);
    }

    public String getPartitionKey() {
        if (partitionKey == null) {
            log.warn(this.name() + " does not have a partition key.");
            return null;
        } else {
            return partitionKey.name();
        }
    }

    public String getRangeKey() {
        if (rangeKey == null) {
            log.warn(this.name() + " does not have a range key.");
            return null;
        } else {
            return rangeKey.name();
        }
    }

    public boolean isHasSignature() {
        return hasSignature;
    }

    public static TableRoleInCollection getByName(String role) {
        for (TableRoleInCollection roleInCollection : values()) {
            if (roleInCollection.name().equalsIgnoreCase(role)) {
                return roleInCollection;
            }
        }
        throw new IllegalArgumentException(String.format("There is no entity name %s in TableRoleInCollection", role));
    }
}
