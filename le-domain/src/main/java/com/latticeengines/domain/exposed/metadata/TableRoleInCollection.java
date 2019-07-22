package com.latticeengines.domain.exposed.metadata;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

public enum TableRoleInCollection {
    ConsolidatedAccount, //
    ConsolidatedContact, //
    ConsolidatedProduct, //
    ConsolidatedRawTransaction, //
    ConsolidatedDailyTransaction, //
    ConsolidatedPeriodTransaction, //

    PivotedRating, //

    Profile, //
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

    // Curated Account Attribute
    // has only requires one table for serving for now.
    CalculatedCuratedAccountAttribute, //

    AnalyticPurchaseState, //

    AccountFeatures, //
    AccountExport, //

    AccountMaster;

    static {
        ConsolidatedAccount.primaryKey = InterfaceName.AccountId;
        ConsolidatedAccount.foreignKeys = ImmutableList.copyOf(Collections.emptyList());
        BucketedAccount.primaryKey = ConsolidatedAccount.primaryKey;
        BucketedAccount.foreignKeys = ConsolidatedAccount.foreignKeys;

        ConsolidatedContact.primaryKey = InterfaceName.ContactId;
        ConsolidatedContact.foreignKeys = ImmutableList.of(InterfaceName.AccountId);
        SortedContact.primaryKey = ConsolidatedContact.primaryKey;
        SortedContact.foreignKeys = ConsolidatedContact.foreignKeys;

        ConsolidatedDailyTransaction.primaryKey = InterfaceName.__Composite_Key__;
        ConsolidatedDailyTransaction.foreignKeys = ImmutableList.of(InterfaceName.AccountId);

        ConsolidatedPeriodTransaction.primaryKey = InterfaceName.__Composite_Key__;
        ConsolidatedPeriodTransaction.foreignKeys = ImmutableList.of(InterfaceName.AccountId);

        AggregatedTransaction.primaryKey = InterfaceName.__Composite_Key__;
        AggregatedTransaction.foreignKeys = ImmutableList.of(InterfaceName.AccountId);

        AggregatedPeriodTransaction.primaryKey = InterfaceName.__Composite_Key__;
        AggregatedPeriodTransaction.foreignKeys = ImmutableList.of(InterfaceName.AccountId);

        ConsolidatedProduct.primaryKey = InterfaceName.ProductId;
        ConsolidatedProduct.foreignKeys = ImmutableList.of(ConsolidatedProduct.primaryKey);
        SortedProduct.primaryKey = ConsolidatedProduct.primaryKey;
        SortedProduct.foreignKeys = ConsolidatedProduct.foreignKeys;
        SortedProductHierarchy.primaryKey = ConsolidatedProduct.primaryKey;
        SortedProductHierarchy.foreignKeys = ConsolidatedProduct.foreignKeys;

        CalculatedPurchaseHistory.primaryKey = InterfaceName.AccountId;
        CalculatedPurchaseHistory.foreignKeys = ImmutableList.copyOf(Collections.emptyList());

        CalculatedDepivotedPurchaseHistory.primaryKey = InterfaceName.__Composite_Key__;
        CalculatedDepivotedPurchaseHistory.foreignKeys = ImmutableList
                .copyOf(Collections.emptyList());

        CalculatedCuratedAccountAttribute.primaryKey = InterfaceName.AccountId;
        CalculatedCuratedAccountAttribute.foreignKeys = ImmutableList
                .copyOf(Collections.emptyList());

        PivotedRating.primaryKey = InterfaceName.AccountId;
        PivotedRating.foreignKeys = ImmutableList.copyOf(Collections.emptyList());

        AccountFeatures.primaryKey = ConsolidatedAccount.primaryKey;
        AccountExport.primaryKey = ConsolidatedAccount.primaryKey;

        AccountMaster.primaryKey = InterfaceName.LatticeAccountId;
    }

    private InterfaceName primaryKey;
    private ImmutableList<InterfaceName> foreignKeys;

    public InterfaceName getPrimaryKey() {
        return primaryKey;
    }

    public List<String> getForeignKeysAsStringList() {
        return foreignKeys.stream().map(InterfaceName::name).collect(Collectors.toList());
    }
}
