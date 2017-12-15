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

    Rating, //

    Profile, //
    ContactProfile, //
    PurchaseHistoryProfile, //

    BucketedAccount, //
    SortedContact, //
    SortedProduct, //
    AggregatedTransaction, //
    AggregatedPeriodTransaction, //
    CalculatedPurchaseHistory, //

    AnalyticPurchaseState, //

    AccountMaster;

    private InterfaceName primaryKey;
    private ImmutableList<InterfaceName> foreignKeys;

    public InterfaceName getPrimaryKey() {
        return primaryKey;
    }

    public List<String> getForeignKeysAsStringList() {
        return foreignKeys.stream().map(InterfaceName::name).collect(Collectors.toList());
    }

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

        AggregatedTransaction.primaryKey = InterfaceName.__Composite_Key__;
        AggregatedTransaction.foreignKeys = ImmutableList.of(InterfaceName.AccountId);

        AggregatedPeriodTransaction.primaryKey = InterfaceName.__Composite_Key__;
        AggregatedPeriodTransaction.foreignKeys = ImmutableList.of(InterfaceName.AccountId);

        ConsolidatedProduct.primaryKey = InterfaceName.ProductId;
        ConsolidatedProduct.foreignKeys = ImmutableList.of(ConsolidatedProduct.primaryKey);
        SortedProduct.primaryKey = ConsolidatedProduct.primaryKey;
        SortedProduct.foreignKeys = ConsolidatedProduct.foreignKeys;

        CalculatedPurchaseHistory.primaryKey = InterfaceName.AccountId;
        CalculatedPurchaseHistory.foreignKeys = ImmutableList.copyOf(Collections.emptyList());

        Rating.primaryKey = InterfaceName.AccountId;
        Rating.foreignKeys = ImmutableList.copyOf(Collections.emptyList());

        AccountMaster.primaryKey = InterfaceName.LatticeAccountId;
    }
}
