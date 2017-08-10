package com.latticeengines.domain.exposed.metadata;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

public enum TableRoleInCollection {
    ConsolidatedAccount, //
    ConsolidatedContact, //

    Profile, //

    BucketedAccount, //
    SortedContact, //
    
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

        AccountMaster.primaryKey = InterfaceName.LatticeAccountId;
    }
}
