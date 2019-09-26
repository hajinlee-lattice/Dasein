package com.latticeengines.domain.exposed.pls;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public enum AtlasExportType {

    ACCOUNT("Enriched Accounts", "Account", BusinessEntity.Account), //
    CONTACT("Enriched Contacts (No Account Attributes)", "Contact", BusinessEntity.Contact), //
    ACCOUNT_AND_CONTACT("Enriched Contacts with Account Attributes", "AccountAndContact",
            BusinessEntity.Contact, BusinessEntity.Account), //
    ACCOUNT_ID("Account_ID", "AccountId", InterfaceName.AccountId, "Account Id"), //
    ALL_ACCOUNTS("All Accounts", "AllAccounts"),
    ALL_CONTACTS("All Contacts", "AllContacts"),
    SEGMENT_ACCOUNTS("Segment Accounts", "SegmentAccounts"),
    SEGMENT_ACCOUNTS_CONTACTS("Segment Accounts Contacts", "SegmentAccountsContacts"),
    ORPHAN_CONTACT("Orphan Contacts", "OrphanContacts", BusinessEntity.Contact, BusinessEntity.Account), //
    ORPHAN_TXN("Orphan Transaction", "OrphanTransaction");

    String displayName;

    String pathFriendlyName;

    public static final Set<AtlasExportType> UI_EXPORT_TYPES = ImmutableSet.of(ACCOUNT, CONTACT);

    List<Triple<BusinessEntity, String, String>> defaultAttributeTuples;

    AtlasExportType(String displayName, String pathFriendlyName) {
        this.displayName = displayName;
        this.pathFriendlyName = pathFriendlyName;
    }

    AtlasExportType(String displayName, String pathFriendlyName, InterfaceName field, String fieldDisplayName) {
        this.displayName = displayName;
        this.pathFriendlyName = pathFriendlyName;
        this.defaultAttributeTuples = Collections.singletonList(
                new ImmutableTriple<>(BusinessEntity.Account, field.name(), fieldDisplayName));
    }

    AtlasExportType(String displayName, String pathFriendlyName, BusinessEntity... entities) {
        this.displayName = displayName;
        this.pathFriendlyName = pathFriendlyName;
        Set<Pair<BusinessEntity, InterfaceName>> attrName = new HashSet<>();
        this.defaultAttributeTuples = Arrays.stream(entities)//
                .map(e -> getDefaultExportAttributesPair(e).stream() //
                        .map(p -> {
                            InterfaceName interfaceName = p.getLeft();
                            Triple<BusinessEntity, String, String> res = null;
                            Pair<BusinessEntity, InterfaceName> pair = Pair.of(e, interfaceName);
                            if (!attrName.contains(pair)) {
                                // give precedence to field from first type
                                // if there are duplicate field names
                                attrName.add(Pair.of(e, interfaceName));
                                res = new ImmutableTriple<>(e, interfaceName.name(), p.getRight());
                            }
                            return res;
                        }) //
                        .filter(r -> r != null) //
                        .collect(Collectors.toList())) //
                .flatMap(Collection::stream) //
                .collect(Collectors.toList());
    }

    public static Set<InterfaceName> getDefaultExportAttributes(BusinessEntity entity, boolean enableEntityMatch) {
        List<Pair<InterfaceName, String>> defaultExportAttributesPair = getDefaultExportAttributesPair(
                entity);
        return defaultExportAttributesPair.stream() //
                .map(pair -> !enableEntityMatch ? (ATTR_MAP.containsKey(pair.getLeft()) ?
                        ATTR_MAP.get(pair.getLeft()) : pair.getLeft()) : (EM_ATTR_MAP.containsKey(pair.getLeft()) ? EM_ATTR_MAP.get(pair.getLeft()) : pair.getLeft()))
                .collect(Collectors.toSet());
    }

    private static List<Pair<InterfaceName, String>> getDefaultExportAttributesPair(
            BusinessEntity entity) {
        List<Pair<InterfaceName, String>> attrs = new ArrayList<>();
        switch (entity) {
            case Account:
                attrs.add(new ImmutablePair<>(InterfaceName.AccountId, "Account Id"));
                attrs.add(new ImmutablePair<>(InterfaceName.CustomerAccountId, "Customer Account Id"));
                attrs.add(new ImmutablePair<>(InterfaceName.CompanyName, "Company Name"));
                attrs.add(new ImmutablePair<>(InterfaceName.Website, "Website"));
                attrs.add(new ImmutablePair<>(InterfaceName.Address_Street_1, "Street"));
                attrs.add(new ImmutablePair<>(InterfaceName.City, "City"));
                attrs.add(new ImmutablePair<>(InterfaceName.State, "State"));
                attrs.add(new ImmutablePair<>(InterfaceName.PostalCode, "Zip"));
                attrs.add(new ImmutablePair<>(InterfaceName.Country, "Country"));
                attrs.add(new ImmutablePair<>(InterfaceName.PhoneNumber, "Phone"));
                break;
            case Contact:
                attrs.add(new ImmutablePair<>(InterfaceName.ContactId, "Contact Id"));
                attrs.add(new ImmutablePair<>(InterfaceName.CustomerContactId, "Customer Contact Id"));
                attrs.add(new ImmutablePair<>(InterfaceName.ContactName, "Contact Name"));
                attrs.add(new ImmutablePair<>(InterfaceName.Email, "Email"));
                attrs.add(new ImmutablePair<>(InterfaceName.PhoneNumber, "Contact Phone"));
                attrs.add(new ImmutablePair<>(InterfaceName.AccountId, "Account Id"));
                attrs.add(new ImmutablePair<>(InterfaceName.CustomerAccountId, "Customer Account Id"));
            default:
        }
        return attrs;
    }

    public static AtlasExportType getByPathName(String pathName) {
        for (AtlasExportType exportType : values()) {
            if (exportType.getPathFriendlyName().equalsIgnoreCase(pathName)) {
                return exportType;
            }
        }
        throw new IllegalArgumentException("Cannot find Atlas Export Type with name: " + pathName);
    }

    public String getDisplayName() {
        return displayName;
    }

    public String getPathFriendlyName() {
        return pathFriendlyName;
    }

    public List<Triple<BusinessEntity, String, String>> getDefaultAttributeTuples() {
        return defaultAttributeTuples;
    }

    // For entity match enabled tenant, legacy AccountId should be replaced by
    // CustomerAccountId, legacy ContactId should be replaced by
    // CustomerContactId
    private static final Map<InterfaceName, InterfaceName> EM_ATTR_MAP = ImmutableMap.of( //
            InterfaceName.AccountId, InterfaceName.CustomerAccountId, //
            InterfaceName.ContactId, InterfaceName.CustomerContactId //
    );

    private static final Map<InterfaceName, InterfaceName> ATTR_MAP = ImmutableMap.of( //
            InterfaceName.CustomerAccountId, InterfaceName.AccountId, //
            InterfaceName.CustomerContactId, InterfaceName.ContactId //
    );
}

