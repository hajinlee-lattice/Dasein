package com.latticeengines.domain.exposed.metadata.standardschemas;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ActivityDate;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ActivityType;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Address_Street_1;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Address_Street_2;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Amount;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.AnnualRevenue;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.AnnualRevenueCurrency;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.City;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CompanyName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ContactCity;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ContactCountry;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ContactId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ContactName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ContactPostalCode;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ContactState;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Contact_Address_Street_1;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Contact_Address_Street_2;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Cost;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Country;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CreatedDate;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CustomTrxField;
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
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Name;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.NumberOfEmployees;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.OpportunityId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.OrderId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.OtherEmail;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.OtherMobileDeviceID;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.OtherPhoneNumber;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PathPattern;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PathPatternName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PhoneNumber;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PostalCode;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PrimaryMobileDeviceID;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ProductId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Quantity;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.SecondaryEmail;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.SecondaryMobileDeviceID;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.SecondaryPhoneNumber;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.SourceMedium;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.SpendAnalyticsSegment;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.StageName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.State;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Title;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.TransactionId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.TransactionTime;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.TransactionType;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Type;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.UserId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.WebVisitDate;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.WebVisitPageUrl;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Website;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;

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
            .map(attr -> ENTITY_MATCH_ATTR_MAP.getOrDefault(attr, attr))
            .collect(Collectors.toList());

    private static final List<InterfaceName> CONTACT_ATTRS = Arrays.asList(ContactId, ContactName, FirstName, LastName,
            AccountId, Title, LeadSource, AnnualRevenue, NumberOfEmployees, Industry, DoNotMail, DoNotCall, LeadStatus,
            LeadType, CreatedDate, LastModifiedDate, Email, CompanyName, Website, Address_Street_1, Address_Street_2,
            City, State, Country, PostalCode, PhoneNumber, DUNS, //
            ContactCity, ContactState, ContactCountry, ContactPostalCode, Contact_Address_Street_1,
            Contact_Address_Street_2, //
            SecondaryEmail, OtherEmail, SecondaryPhoneNumber, OtherPhoneNumber, //
            PrimaryMobileDeviceID, SecondaryMobileDeviceID, OtherMobileDeviceID //
    );

    private static final List<InterfaceName> CONTACT_ENTITY_MATCH_ATTRS = CONTACT_ATTRS.stream()
            .map(attr -> ENTITY_MATCH_ATTR_MAP.getOrDefault(attr, attr))
            .collect(Collectors.toList());

    private static final List<InterfaceName> TRANSACTION_ATTRS = Arrays.asList(TransactionId, AccountId, ContactId,
            ProductId, OrderId, LastModifiedDate, Quantity, Amount, Cost, TransactionTime, TransactionType,
            CustomTrxField);

    private static final List<InterfaceName> TRANSACTION_ENTITY_MATCH_ATTRS = TRANSACTION_ATTRS.stream() //
            .map(attr -> ENTITY_MATCH_ATTR_MAP.getOrDefault(attr, attr)) //
            .collect(Collectors.toList());

    private static final List<InterfaceName> WEBVISIT_ATTRS = Arrays.asList(WebVisitPageUrl, WebVisitDate, UserId, //
            SourceMedium, CompanyName, Website, City, State, Country, PostalCode, DUNS);

    private static final List<InterfaceName> WEBVISIT_PATHPATTERN_ATTRS = Arrays.asList(PathPatternName, PathPattern);

    private static final List<InterfaceName> WEBVISIT_SOURCE_MEDIUM = Arrays.asList(SourceMedium);

    private static final List<InterfaceName> OPPORTUNITY = Arrays.asList(LastModifiedDate, OpportunityId, StageName);
    private static final List<InterfaceName> OPPORTUNITY_STAGE = Collections.singletonList(StageName);

    private static final List<InterfaceName> MARKETING_ACTIVITY = Arrays.asList(ActivityDate, ActivityType);
    private static final List<InterfaceName> MARKETING_ACTIVITY_TYPE = Arrays.asList(Name, ActivityType);
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
        Table table = sr.getSchema(schema, false, false, enableEntityMatch, false);
        String[] attrArray = table.getAttributeNames();
        Arrays.sort(attrArray);
        String[] expectedAttrArray = expectedAttrs.stream().map(Enum::name).sorted().toArray(String[]::new);
        Assert.assertEquals(attrArray, expectedAttrArray);
        if (expectedPrimaryKey == null) {
            Assert.assertNull(table.getPrimaryKey());
        } else {
            Assert.assertEquals(table.getPrimaryKey().getAttributes().get(0), expectedPrimaryKey.name());
        }
    }

    @Test(groups = "unit")
    public void testSchemaAttrUniqueness() {
        boolean[] bools = new boolean[] {true, false};
        SchemaRepository sr = SchemaRepository.instance();
        for (BusinessEntity entity : BusinessEntity.values()) {
            for (boolean cdlSchema : bools) {
                for (boolean enableEntityMatch : bools) {
                    try {
                        // withoutId will be retired
                        Table table = sr.getSchema(entity, cdlSchema, false, enableEntityMatch, false);
                        Assert.assertEquals(Stream.of(table.getAttributeNames()).distinct().count(),
                                table.getAttributeNames().length);
                    } catch (Exception ex) {
                        Assert.assertTrue(ex instanceof RuntimeException);
                        Assert.assertEquals(ex.getMessage(), String.format("Unsupported schema %s", entity));
                    }
                }
            }
        }

        for (SchemaInterpretation schema : SchemaInterpretation.values()) {
            for (boolean includeCdlTimestamps : bools) {
                for (boolean enableEntityMatch : bools) {
                    try {
                        // withoutId will be retired
                        Table table = sr.getSchema(schema, includeCdlTimestamps, false, enableEntityMatch, false);
                        Assert.assertEquals(Stream.of(table.getAttributeNames()).distinct().count(),
                                table.getAttributeNames().length);
                    } catch (Exception ex) {
                        Assert.assertTrue(ex instanceof RuntimeException);
                        Assert.assertEquals(ex.getMessage(), String.format("Unsupported schema %s", schema));
                    }
                }
            }
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
                { SchemaInterpretation.Transaction, true, TRANSACTION_ENTITY_MATCH_ATTRS, null }, //
                { SchemaInterpretation.Transaction, false, TRANSACTION_ATTRS, null }, //
        };
    }

    @Test(groups = "unit", dataProvider = "entityTypeProvider")
    public void testGetSchemaByEntityType(S3ImportSystem.SystemType systemType, EntityType entityType,
                                          boolean enableEntityMatch, boolean enableEntityMatchGA,
                                          List<InterfaceName> expectedAttrs) {
        Table table = SchemaRepository.instance().getSchema(systemType, entityType, enableEntityMatch, enableEntityMatchGA);
        String[] attrArray = table.getAttributeNames();
        Arrays.sort(attrArray);
        String[] expectedAttrArray = expectedAttrs.stream().map(Enum::name).sorted().toArray(String[]::new);
        Assert.assertEquals(attrArray, expectedAttrArray);
    }


    @DataProvider(name = "entityTypeProvider")
    public Object[][] entityTypeProvider() {
        return new Object[][] {
                { S3ImportSystem.SystemType.Other, EntityType.Accounts, true, false, ACCOUNT_ENTITY_MATCH_ATTRS }, //
                { S3ImportSystem.SystemType.Other, EntityType.Accounts, false, false, ACCOUNT_ATTRS }, //
                { S3ImportSystem.SystemType.Other, EntityType.Contacts, true, false, CONTACT_ENTITY_MATCH_ATTRS }, //
                { S3ImportSystem.SystemType.Other, EntityType.Contacts, false, false, CONTACT_ATTRS }, //
                { S3ImportSystem.SystemType.Other, EntityType.ProductPurchases, true, false, TRANSACTION_ENTITY_MATCH_ATTRS }, //
                { S3ImportSystem.SystemType.Other, EntityType.ProductPurchases, true, true, TRANSACTION_ENTITY_MATCH_ATTRS }, //
                { S3ImportSystem.SystemType.Other, EntityType.ProductPurchases, false, false, TRANSACTION_ATTRS }, //
                { S3ImportSystem.SystemType.Other, EntityType.WebVisit, true, false, WEBVISIT_ATTRS }, //
                { S3ImportSystem.SystemType.Other, EntityType.WebVisitPathPattern, true, false, WEBVISIT_PATHPATTERN_ATTRS }, //
                { S3ImportSystem.SystemType.Other, EntityType.WebVisitSourceMedium, true, false, WEBVISIT_SOURCE_MEDIUM }, //
                { S3ImportSystem.SystemType.Other, EntityType.Opportunity, true, false, OPPORTUNITY }, //
                { S3ImportSystem.SystemType.Other, EntityType.OpportunityStageName, true, false, OPPORTUNITY_STAGE }, //
                { S3ImportSystem.SystemType.Other, EntityType.MarketingActivity, true, false, MARKETING_ACTIVITY}, //
                { S3ImportSystem.SystemType.Other, EntityType.MarketingActivityType, true, false, MARKETING_ACTIVITY_TYPE}, //
        };
    }
}
