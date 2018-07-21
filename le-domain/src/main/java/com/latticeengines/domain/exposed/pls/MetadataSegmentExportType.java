package com.latticeengines.domain.exposed.pls;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public enum MetadataSegmentExportType {

    ACCOUNT("Accounts", BusinessEntity.Account), //
    CONTACT("Contacts", BusinessEntity.Contact), //
    ACCOUNT_AND_CONTACT("Accounts and Contacts", BusinessEntity.Contact, BusinessEntity.Account), //
    ACCOUNT_ID("Account_ID", InterfaceName.AccountId, "Account Id"); // ;

    String displayName;

    List<Triple<BusinessEntity, String, String>> defaultAttributeTuples;

    MetadataSegmentExportType(String displayName, InterfaceName field, String fieldDisplayName) {
        this.displayName = displayName;
        this.defaultAttributeTuples = Collections
                .singletonList(new ImmutableTriple<>(BusinessEntity.Account, field.name(), fieldDisplayName));
    }

    MetadataSegmentExportType(String displayName, BusinessEntity... entities) {
        this.displayName = displayName;
        Set<InterfaceName> attrName = new HashSet<>();
        this.defaultAttributeTuples = Arrays.asList(entities).stream() //
                .map(e -> {
                    return getDefaultExportAttributesPair(e).stream() //
                            .map(p -> {
                                InterfaceName interfaceName = p.getLeft();
                                Triple<BusinessEntity, String, String> res = null;
                                if (!attrName.contains(interfaceName)) {
                                    // give precedence to field from first type
                                    // if there are duplicate field names
                                    attrName.add(interfaceName);
                                    res = new ImmutableTriple<>(e, interfaceName.name(), p.getRight());
                                }
                                return res;
                            }) //
                            .filter(r -> r != null) //
                            .collect(Collectors.toList());
                }) //
                .flatMap(Collection::stream) //
                .collect(Collectors.toList());
    }

    public String getDisplayName() {
        return displayName;
    }

    public List<Triple<BusinessEntity, String, String>> getDefaultAttributeTuples() {
        return defaultAttributeTuples;
    }

    public static Set<InterfaceName> getDefaultExportAttributes(BusinessEntity entity) {
        List<Pair<InterfaceName, String>> defaultExportAttributesPair = getDefaultExportAttributesPair(entity);
        return defaultExportAttributesPair.stream() //
                .map(Pair::getLeft) //
                .collect(Collectors.toSet());
    }

    private static List<Pair<InterfaceName, String>> getDefaultExportAttributesPair(BusinessEntity entity) {
        List<Pair<InterfaceName, String>> attrs = new ArrayList<>();
        switch (entity) {
        case Account:
            attrs.add(new ImmutablePair<InterfaceName, String>(InterfaceName.AccountId, "Account Id"));
            attrs.add(new ImmutablePair<InterfaceName, String>(InterfaceName.CompanyName, "Company Name"));
            attrs.add(new ImmutablePair<InterfaceName, String>(InterfaceName.Website, "Website"));
            attrs.add(new ImmutablePair<InterfaceName, String>(InterfaceName.Address_Street_1, "Street"));
            attrs.add(new ImmutablePair<InterfaceName, String>(InterfaceName.City, "City"));
            attrs.add(new ImmutablePair<InterfaceName, String>(InterfaceName.State, "State"));
            attrs.add(new ImmutablePair<InterfaceName, String>(InterfaceName.PostalCode, "Zip"));
            attrs.add(new ImmutablePair<InterfaceName, String>(InterfaceName.Country, "Country"));
            attrs.add(new ImmutablePair<InterfaceName, String>(InterfaceName.PhoneNumber, "Phone"));
            break;
        case Contact:
            attrs.add(new ImmutablePair<InterfaceName, String>(InterfaceName.ContactId, "Contact Id"));
            attrs.add(new ImmutablePair<InterfaceName, String>(InterfaceName.ContactName, "Contact Name"));
            attrs.add(new ImmutablePair<InterfaceName, String>(InterfaceName.Email, "Email"));
            attrs.add(new ImmutablePair<InterfaceName, String>(InterfaceName.PhoneNumber, "Contact Phone"));
            attrs.add(new ImmutablePair<InterfaceName, String>(InterfaceName.AccountId, "Account Id"));
        default:
        }
        return attrs;
    }
}
