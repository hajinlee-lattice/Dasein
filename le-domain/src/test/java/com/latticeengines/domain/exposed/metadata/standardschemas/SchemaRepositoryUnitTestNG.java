package com.latticeengines.domain.exposed.metadata.standardschemas;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Address_Street_1;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Address_Street_2;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Amount;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.AnnualRevenue;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.AnnualRevenueCurrency;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.City;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CompanyName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ContactId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ContactName;
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
import static com.latticeengines.domain.exposed.metadata.InterfaceName.NumberOfEmployees;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.OrderId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PathPattern;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PathPatternName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PhoneNumber;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PostalCode;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ProductId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Quantity;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.SpendAnalyticsSegment;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.State;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Title;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.TransactionId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.TransactionTime;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.TransactionType;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Type;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.UserId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.WebVisitPageUrl;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Website;

import java.util.Arrays;
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
            City, State, Country, PostalCode, PhoneNumber, DUNS);

    private static final List<InterfaceName> CONTACT_ENTITY_MATCH_ATTRS = CONTACT_ATTRS.stream()
            .map(attr -> ENTITY_MATCH_ATTR_MAP.getOrDefault(attr, attr))
            .collect(Collectors.toList());

    private static final List<InterfaceName> TRANSACTION_ATTRS = Arrays.asList(TransactionId, AccountId, ContactId,
            ProductId, OrderId, LastModifiedDate, Quantity, Amount, Cost, TransactionTime, TransactionType,
            CustomTrxField);

    private static final List<InterfaceName> TRANSACTION_ENTITY_MATCH_ATTRS = TRANSACTION_ATTRS.stream() //
            .map(attr -> ENTITY_MATCH_ATTR_MAP.getOrDefault(attr, attr)) //
            .collect(Collectors.toList());

    private static final List<InterfaceName> WEBVISIT_ATTRS = Arrays.asList(WebVisitPageUrl, UserId, CompanyName,
            City, State, Country, DUNS);

    private static final List<InterfaceName> WEBVISIT_PATHPATTERN_ATTRS = Arrays.asList(PathPatternName, PathPattern);
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
                        Table table = sr.getSchema(entity, cdlSchema, false, enableEntityMatch);
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
                        Table table = sr.getSchema(schema, includeCdlTimestamps, false, enableEntityMatch);
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
                                          boolean enableEntityMatch,
                                          List<InterfaceName> expectedAttrs) {
        Table table = SchemaRepository.instance().getSchema(systemType, entityType, enableEntityMatch);
        String[] attrArray = table.getAttributeNames();
        Arrays.sort(attrArray);
        String[] expectedAttrArray = expectedAttrs.stream().map(Enum::name).sorted().toArray(String[]::new);
        Assert.assertEquals(attrArray, expectedAttrArray);
    }


    @DataProvider(name = "entityTypeProvider")
    public Object[][] entityTypeProvider() {
        return new Object[][] {
                {S3ImportSystem.SystemType.Other, EntityType.Accounts, true, ACCOUNT_ENTITY_MATCH_ATTRS }, //
                {S3ImportSystem.SystemType.Other, EntityType.Accounts, false, ACCOUNT_ATTRS }, //
                {S3ImportSystem.SystemType.Other, EntityType.Contacts, true, CONTACT_ENTITY_MATCH_ATTRS }, //
                {S3ImportSystem.SystemType.Other, EntityType.Contacts, false, CONTACT_ATTRS }, //
                {S3ImportSystem.SystemType.Other, EntityType.ProductPurchases, true, TRANSACTION_ENTITY_MATCH_ATTRS }, //
                {S3ImportSystem.SystemType.Other, EntityType.ProductPurchases, false, TRANSACTION_ATTRS }, //
                {S3ImportSystem.SystemType.Other, EntityType.WebVisit, true, WEBVISIT_ATTRS }, //
                {S3ImportSystem.SystemType.Other, EntityType.WebVisitPathPattern, true, WEBVISIT_PATHPATTERN_ATTRS }, //
        };
    }
}
