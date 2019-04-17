package com.latticeengines.domain.exposed.metadata.standardschemas;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Address_Street_1;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Address_Street_2;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.AnnualRevenue;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.AnnualRevenueCurrency;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.City;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CompanyName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ContactId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ContactName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Country;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CreatedDate;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CustomerAccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CustomerContactId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CustomerParentAccountID;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.DUNS;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.DoNotCall;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.DoNotMail;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Email;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Event;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.FirstName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Industry;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.LastModifiedDate;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.LastName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Latitude;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.LeadSource;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.LeadStatus;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.LeadType;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Longitude;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.NumberOfEmployees;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PhoneNumber;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PostalCode;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.SpendAnalyticsSegment;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.State;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Title;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Type;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Website;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;

public class SchemaRepositoryUnitTestNG {

    // Attr without EntityMatch -> Attr with EntityMatch
    private static final Map<InterfaceName, InterfaceName> ENTITY_MATCH_ATTR_MAP = ImmutableMap.of( //
            AccountId, CustomerAccountId, //
            ContactId, CustomerContactId //
    );

    private static final List<InterfaceName> ACCOUNT_ATTRS = Arrays.asList(AccountId, Industry, AnnualRevenue,
            NumberOfEmployees, Type, AnnualRevenueCurrency, SpendAnalyticsSegment, CustomerParentAccountID, Longitude,
            Latitude, Event, Website, CompanyName, DUNS, City, State, Country, PostalCode, PhoneNumber,
            Address_Street_1, Address_Street_2);

    private static final List<InterfaceName> ACCOUNT_ENTITY_MATCH_ATTRS = ACCOUNT_ATTRS.stream()
            .map(attr -> ENTITY_MATCH_ATTR_MAP.containsKey(attr) ? ENTITY_MATCH_ATTR_MAP.get(attr) : attr)
            .collect(Collectors.toList());

    private static final List<InterfaceName> CONTACT_ATTRS = Arrays.asList(ContactId, ContactName, FirstName, LastName,
            AccountId, Title, LeadSource, AnnualRevenue, NumberOfEmployees, Industry, DoNotMail, DoNotCall, LeadStatus,
            LeadType, CreatedDate, LastModifiedDate, Email, CompanyName, Website, Address_Street_1, Address_Street_2,
            City, State, Country, PostalCode, PhoneNumber, DUNS);

    private static final List<InterfaceName> CONTACT_ENTITY_MATCH_ATTRS = CONTACT_ATTRS.stream()
            .map(attr -> ENTITY_MATCH_ATTR_MAP.containsKey(attr) ? ENTITY_MATCH_ATTR_MAP.get(attr) : attr)
            .collect(Collectors.toList());

    /**
     * Currently only covers testing Account & Contact schema with
     * enableEntityMatch turned on and off
     *
     * @param schema
     * @param enableEntityMatch
     */
    @Test(groups = "unit", dataProvider = "schemaProvider")
    public void testGetSchema(SchemaInterpretation schema, boolean enableEntityMatch, List<InterfaceName> expectedAttrs,
            InterfaceName expectedPrimaryKey) {
        SchemaRepository sr = SchemaRepository.instance();
        Table table = sr.getSchema(schema, false, false, enableEntityMatch);
        String[] attrArray = table.getAttributeNames();
        Arrays.sort(attrArray);
        String[] expectedAttrArray = expectedAttrs.stream().map(attr -> attr.name()).collect(Collectors.toList())
                .toArray(new String[0]);
        Arrays.sort(expectedAttrArray);
        Assert.assertEquals(attrArray, expectedAttrArray);
        if (expectedPrimaryKey == null) {
            Assert.assertNull(table.getPrimaryKey());
        } else {
            Assert.assertEquals(table.getPrimaryKey().getAttributes().get(0), expectedPrimaryKey.name());
        }
    }

    // schemaInterpretation, enableEntityMatch, expectedAttrs,
    // expectedPrimaryKey
    @DataProvider(name = "schemaProvider")
    public Object[][] provideSchemas() {
        return new Object[][] {
                { SchemaInterpretation.Account, true, ACCOUNT_ENTITY_MATCH_ATTRS, null }, //
                { SchemaInterpretation.Account, false, ACCOUNT_ATTRS, AccountId }, //
                { SchemaInterpretation.Contact, true, CONTACT_ENTITY_MATCH_ATTRS, null }, //
                { SchemaInterpretation.Contact, false, CONTACT_ATTRS, ContactId }, //
        };
    }

}
