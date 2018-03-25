package com.latticeengines.domain.exposed.pls;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.domain.exposed.metadata.InterfaceName;

public enum MetadataSegmentExportType {
    ACCOUNT("Accounts", //
            Arrays.asList(//
                    MetadataSegmentExport.ACCOUNT_PREFIX + InterfaceName.AccountId.name(), //
                    MetadataSegmentExport.ACCOUNT_PREFIX + InterfaceName.LDC_Name.name(), //
                    MetadataSegmentExport.ACCOUNT_PREFIX + InterfaceName.Website.name(), //
                    MetadataSegmentExport.ACCOUNT_PREFIX + InterfaceName.Address_Street_1.name().toLowerCase(), //
                    MetadataSegmentExport.ACCOUNT_PREFIX + InterfaceName.City.name(), //
                    MetadataSegmentExport.ACCOUNT_PREFIX + InterfaceName.State.name(), //
                    MetadataSegmentExport.ACCOUNT_PREFIX + InterfaceName.PostalCode.name(), //
                    MetadataSegmentExport.ACCOUNT_PREFIX + InterfaceName.Country.name(), //
                    MetadataSegmentExport.ACCOUNT_PREFIX + InterfaceName.PhoneNumber.name(), //
                    MetadataSegmentExport.ACCOUNT_PREFIX + InterfaceName.SalesforceAccountID.name() //
            ), //
            Arrays.asList("Account Id", "Company Name", "Website", "Street", //
                    "City", "State", "Zip", "Country", "Phone", "Salesforce Id")), //
    CONTACT("Contacts", //
            Arrays.asList(//
                    MetadataSegmentExport.CONTACT_PREFIX + InterfaceName.ContactId.name(), //
                    MetadataSegmentExport.CONTACT_PREFIX + InterfaceName.ContactName.name(), //
                    MetadataSegmentExport.CONTACT_PREFIX + InterfaceName.Email.name(), //
                    MetadataSegmentExport.CONTACT_PREFIX + InterfaceName.PhoneNumber.name(), //
                    MetadataSegmentExport.CONTACT_PREFIX + InterfaceName.AccountId.name() //
            ), //
            Arrays.asList("Contact Id", "Contact Name", "Email", "Contact Phone", "Account Id")), //
    ACCOUNT_AND_CONTACT("Accounts and Contacts", //
            Arrays.asList( //
                    MetadataSegmentExport.CONTACT_PREFIX + InterfaceName.ContactId.name(), //
                    MetadataSegmentExport.CONTACT_PREFIX + InterfaceName.ContactName.name(), //
                    MetadataSegmentExport.CONTACT_PREFIX + InterfaceName.Email.name(), //
                    MetadataSegmentExport.CONTACT_PREFIX + InterfaceName.PhoneNumber.name(), //
                    MetadataSegmentExport.CONTACT_PREFIX + InterfaceName.AccountId.name(), //
                    //
                    MetadataSegmentExport.ACCOUNT_PREFIX + InterfaceName.LDC_Name.name(), //
                    MetadataSegmentExport.ACCOUNT_PREFIX + InterfaceName.Website.name(), //
                    MetadataSegmentExport.ACCOUNT_PREFIX + InterfaceName.Address_Street_1.name().toLowerCase(), //
                    MetadataSegmentExport.ACCOUNT_PREFIX + InterfaceName.City.name(), //
                    MetadataSegmentExport.ACCOUNT_PREFIX + InterfaceName.State.name(), //
                    MetadataSegmentExport.ACCOUNT_PREFIX + InterfaceName.PostalCode.name(), //
                    MetadataSegmentExport.ACCOUNT_PREFIX + InterfaceName.Country.name(), //
                    MetadataSegmentExport.ACCOUNT_PREFIX + InterfaceName.SalesforceAccountID.name() //
            ), //
            Arrays.asList("Contact Id", "Contact Name", "Email", "Contact Phone", "Account Id", //
                    "Company Name", "Website", "Street", "City", "State", "Zip", "Country", "Salesforce Id")), //
    ACCOUNT_ID("Account_ID", //
            Collections.singletonList(InterfaceName.AccountId.name()), //
            Collections.singletonList("Account Id")); // ;

    String displayName;

    List<Pair<String, String>> fieldNamePairs;

    MetadataSegmentExportType(String displayName, List<String> fieldNames, List<String> fieldDisplayNames) {
        this.displayName = displayName;
        this.fieldNamePairs = new ArrayList<>();
        for (int i = 0; i < fieldNames.size(); i++) {
            fieldNamePairs.add( //
                    new ImmutablePair<String, String>( //
                            fieldNames.get(i), //
                            fieldDisplayNames.get(i)));
        }
    }

    public String getDisplayName() {
        return displayName;
    }

    public List<Pair<String, String>> getFieldNamePairs() {
        return fieldNamePairs;
    }
}
