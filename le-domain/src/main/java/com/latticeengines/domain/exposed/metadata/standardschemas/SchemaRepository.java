package com.latticeengines.domain.exposed.metadata.standardschemas;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.joda.time.DateTime;

import com.google.common.collect.Sets;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LastModifiedKey;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.PrimaryKey;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.metadata.validators.RequiredIfOtherFieldIsEmpty;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class SchemaRepository {
    private static SchemaRepository instance;

    public static SchemaRepository instance() {
        if (instance == null) {
            synchronized (SchemaRepository.class) {
                if (instance == null) {
                    instance = new SchemaRepository();
                }
            }
        }
        return instance;
    }

    private SchemaRepository() {
    }

    public Table getSchema(BusinessEntity entity) {
        switch (entity) {
        case Account:
            return getAccountSchema();
        case Contact:
            return getContactSchema();
        case Product:
            return getProductSchema();
        case Transaction:
            return getTransactionSchema();
        default:
            throw new RuntimeException(String.format("Unsupported schema %s", entity));
        }
    }

    public Table getSchema(SchemaInterpretation schema) {
        switch (schema) {
        case SalesforceAccount:
            return getSalesforceAccountSchema();
        case SalesforceLead:
            return getSalesforceLeadSchema();
        case Account:
            return getAccountSchema();
        case Contact:
            return getContactSchema();
        case Product:
            return getProductSchema();
        case TimeSeries:
            return getTimeSeriesSchema();
        case Category:
            return getCategorySchema();
        case Transaction:
            return getTransactionSchema();
        case TransactionDailyAggregation:
            return getTransactionDailyAggregationSchema();
        default:
            throw new RuntimeException(String.format("Unsupported schema %s", schema));
        }
    }

    private Table getCategorySchema() {
        Table table = createTable(SchemaInterpretation.Category);
        table.setLastModifiedKey(createLastModifiedKey("LastModifiedDate"));
        table.setPrimaryKey(createPrimaryKey("Id"));

        table.addAttribute(attr("CategoryId") //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "CATEGORY", "CATEGORY ID" })) //
                .type(Schema.Type.STRING) //
                .required() //
                .interfaceName(InterfaceName.Id) //
                .logicalType(LogicalDataType.Reference) //
                .fundamentalType(FundamentalType.ALPHA.name()) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());
        table.addAttribute(attr("SubcategoryId") //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "SUBCATEGORY", "SUBCATEGORY ID" })) //
                .type(Schema.Type.STRING) //
                .required() //
                .interfaceName(InterfaceName.Id) //
                .logicalType(LogicalDataType.Id) //
                .fundamentalType(FundamentalType.ALPHA.name()) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());
        table.addAttribute(attr("Description") //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "DESCRIPTION" })) //
                .type(Schema.Type.STRING) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .category(ModelingMetadata.CATEGORY_LEAD_INFORMATION) //
                .statisticalType(ModelingMetadata.NOMINAL_STAT_TYPE) //
                .build());
        table.addAttribute(attr("LastModifiedDate") //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "LASTMODIFIEDDATE", "LAST MODIFIED DATE" })) //
                .type(Schema.Type.LONG) //
                .required() //
                .interfaceName(InterfaceName.LastModifiedDate) //
                .logicalType(LogicalDataType.Date) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_YEAR) //
                .category(ModelingMetadata.CATEGORY_LEAD_INFORMATION) //
                .build());

        return table;
    }

    private Table getTimeSeriesSchema() {
        Table table = createTable(SchemaInterpretation.TimeSeries);
        table.setLastModifiedKey(createLastModifiedKey("Timestamp"));

        /*
         * Each transaction row has to be associated with an account keyed off
         * the following: 1. AccountId - could be SFDC external id 2.
         * CompanyName+Location 3. DUNS number
         */

        table.addAttribute(attr("AccountId") //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "ACCOUNT_ID", "ACCOUNTID" })) //
                .type(Schema.Type.STRING) //
                .required() //
                .interfaceName(InterfaceName.AccountId) //
                .logicalType(LogicalDataType.Id) //
                .fundamentalType(FundamentalType.ALPHA.name()) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());
        table.addAttribute(attr("SubcategoryId") //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "SUBCATEGORY_ID" })) //
                .type(Schema.Type.STRING) //
                .required() //
                .interfaceName(InterfaceName.SubcategoryId) //
                .logicalType(LogicalDataType.Reference) //
                .fundamentalType(FundamentalType.ALPHA.name()) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());
        table.addAttribute(attr("Timestamp") //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "TIMESTAMP", "TIME STAMP" })) //
                .type(Schema.Type.LONG) //
                .required() //
                .interfaceName(InterfaceName.LastModifiedDate) //
                .logicalType(LogicalDataType.Timestamp) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_YEAR) //
                .category(ModelingMetadata.CATEGORY_LEAD_INFORMATION) //
                .build());
        table.addAttribute(attr("Quantity") //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "QUANTITY" })) //
                .type(Schema.Type.LONG) //
                .required() //
                .interfaceName(InterfaceName.Quantity) //
                .logicalType(LogicalDataType.Metric) //
                .approvedUsage(ModelingMetadata.MODEL_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_NUMERIC) //
                .category(ModelingMetadata.CATEGORY_LEAD_INFORMATION) //
                .build());
        table.addAttribute(attr("Amount") //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "AMOUNT" })) //
                .type(Schema.Type.LONG) //
                .required() //
                .interfaceName(InterfaceName.Amount) //
                .logicalType(LogicalDataType.Metric) //
                .approvedUsage(ModelingMetadata.MODEL_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_NUMERIC) //
                .category(ModelingMetadata.CATEGORY_LEAD_INFORMATION) //
                .build());

        return table;
    }

    private Table getSalesforceAccountSchema() {
        Table table = createTable(SchemaInterpretation.SalesforceAccount);
        table.setLastModifiedKey(createLastModifiedKey("LastModifiedDate"));
        table.setPrimaryKey(createPrimaryKey("Id"));

        table.addAttribute(attr("Id") //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "ID", "ACCOUNT", "ACCOUNT ID" })) //
                .type(Schema.Type.STRING) //
                .required() //
                .interfaceName(InterfaceName.Id) //
                .logicalType(LogicalDataType.Id) //
                .fundamentalType(FundamentalType.ALPHA.name()) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());

        table.addAttribute(attr("Event") //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "EVENT", "WON", "P1_EVENT" })) //
                .type(Schema.Type.BOOLEAN) //
                .required() //
                .interfaceName(InterfaceName.Event) //
                .logicalType(LogicalDataType.Event) //
                .fundamentalType(ModelingMetadata.FT_BOOLEAN) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());

        table.addAttribute(attr("Industry") //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "INDUSTRY" })) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.Industry) //
                .approvedUsage(ModelingMetadata.MODEL_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());
        table.addAttribute(attr("AnnualRevenue") //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "ANNUALREVENUE", "ANNUAL REVENUE" })) //
                .type(Schema.Type.DOUBLE) //
                .interfaceName(InterfaceName.AnnualRevenue) //
                .approvedUsage(ModelingMetadata.MODEL_AND_ALL_INSIGHTS_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_NUMERIC) //
                .statisticalType(ModelingMetadata.RATIO_STAT_TYPE) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());
        table.addAttribute(attr("NumberOfEmployees") //
                .allowedDisplayNames(
                        Sets.newHashSet(new String[] { "NUMBEROFEMPLOYEES", "NUMBER OF EMPLOYEES", "EMPLOYEES" })) //
                .type(Schema.Type.INT) //
                .interfaceName(InterfaceName.NumberOfEmployees) //
                .approvedUsage(ModelingMetadata.MODEL_AND_ALL_INSIGHTS_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_NUMERIC) //
                .statisticalType(ModelingMetadata.RATIO_STAT_TYPE) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());
        table.addAttribute(attr("CreatedDate") //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "CREATEDDATE", "CREATED DATE" })) //
                .type(Schema.Type.LONG) //
                .interfaceName(InterfaceName.CreatedDate) //
                .logicalType(LogicalDataType.Date) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_YEAR) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());
        table.addAttribute(attr("LastModifiedDate") //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "LASTMODIFIEDDATE", "LAST MODIFIED DATE" })) //
                .type(Schema.Type.LONG) //
                .interfaceName(InterfaceName.LastModifiedDate) //
                .logicalType(LogicalDataType.Date) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_YEAR) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());
        table.addAttribute(attr("YearStarted") //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "YEARSTARTED", "YEAR STARTED" })) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.YearStarted) //
                .approvedUsage(ModelingMetadata.MODEL_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_YEAR) //
                .statisticalType(ModelingMetadata.NOMINAL_STAT_TYPE) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());
        table.addAttribute(attr("IsClosed") //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "ISCLOSED", "IS CLOSED", "CLOSED" })) //
                .type(Schema.Type.BOOLEAN) //
                .interfaceName(InterfaceName.IsClosed) //
                .logicalType(LogicalDataType.Opportunity) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_BOOLEAN) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());
        table.addAttribute(attr("StageName") //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "STAGE NAME", "STAGE" })) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.StageName) //
                .logicalType(LogicalDataType.Opportunity) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());

        return table;
    }

    private Table getSalesforceLeadSchema() {
        Table table = createTable(SchemaInterpretation.SalesforceLead);
        table.setLastModifiedKey(createLastModifiedKey("LastModifiedDate"));
        table.setPrimaryKey(createPrimaryKey("Id"));

        table.addAttribute(attr("Id") //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "ID", "LEAD", "LEAD ID" })) //
                .type(Schema.Type.STRING) //
                .required() //
                .interfaceName(InterfaceName.Id) //
                .logicalType(LogicalDataType.Id) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());

        table.addAttribute(attr("Event") //
                .type(Schema.Type.BOOLEAN) //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "EVENT", "WON", "P1_EVENT" })) //
                .required()//
                .interfaceName(InterfaceName.Event) //
                .logicalType(LogicalDataType.Event) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_BOOLEAN) //
                .build());

        table.addAttribute(attr("CreatedDate") //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "CREATEDDATE", "CREATED DATE" })) //
                .type(Schema.Type.LONG) //
                .interfaceName(InterfaceName.CreatedDate) //
                .logicalType(LogicalDataType.Date) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_YEAR) //
                .category(ModelingMetadata.CATEGORY_LEAD_INFORMATION) //
                .build());
        table.addAttribute(attr("LastModifiedDate") //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "LASTMODIFIEDDATE", "LAST MODIFIED DATE" })) //
                .type(Schema.Type.LONG) //
                .interfaceName(InterfaceName.LastModifiedDate) //
                .logicalType(LogicalDataType.Date) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_YEAR) //
                .category(ModelingMetadata.CATEGORY_LEAD_INFORMATION) //
                .build());
        table.addAttribute(attr("FirstName") //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "FIRSTNAME", "FIRST NAME" })) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.FirstName) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .category(ModelingMetadata.CATEGORY_LEAD_INFORMATION) //
                .build());
        table.addAttribute(attr("LastName") //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "LASTNAME", "LAST NAME" })) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.LastName) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .category(ModelingMetadata.CATEGORY_LEAD_INFORMATION) //
                .build());
        table.addAttribute(attr("Title") //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "TITLE" })) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.Title) //
                .approvedUsage(ModelingMetadata.MODEL_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .category(ModelingMetadata.CATEGORY_LEAD_INFORMATION) //
                .build());
        table.addAttribute(attr("LeadSource") //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "LEADSOURCE", "LEAD SOURCE" })) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.LeadSource) //
                .approvedUsage(ModelingMetadata.MODEL_AND_ALL_INSIGHTS_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .statisticalType(ModelingMetadata.NOMINAL_STAT_TYPE) //
                .category(ModelingMetadata.CATEGORY_LEAD_INFORMATION) //
                .build());
        table.addAttribute(attr("IsClosed") //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "ISCLOSED", "IS CLOSED", "CLOSED" })) //
                .type(Schema.Type.BOOLEAN) //
                .interfaceName(InterfaceName.IsClosed) //
                .logicalType(LogicalDataType.Opportunity) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_BOOLEAN) //
                .category(ModelingMetadata.CATEGORY_LEAD_INFORMATION) //
                .build());
        table.addAttribute(attr("StageName") //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "STAGE NAME", "STAGE" })) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.StageName) //
                .logicalType(LogicalDataType.Opportunity) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .category(ModelingMetadata.CATEGORY_LEAD_INFORMATION) //
                .build());
        table.addAttribute(attr("AnnualRevenue") //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "ANNUALREVENUE", "ANNUAL REVENUE" })) //
                .type(Schema.Type.DOUBLE) //
                .interfaceName(InterfaceName.AnnualRevenue) //
                .approvedUsage(ModelingMetadata.MODEL_AND_ALL_INSIGHTS_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_NUMERIC) //
                .statisticalType(ModelingMetadata.RATIO_STAT_TYPE) //
                .category(ModelingMetadata.CATEGORY_LEAD_INFORMATION) //
                .build());
        table.addAttribute(attr("NumberOfEmployees") //
                .allowedDisplayNames(Sets
                        .newHashSet(new String[] { "NUMBEROFEMPLOYEES", "NUMBER OF EMPLOYEES", "NO. OF EMPLOYEES" })) //
                .type(Schema.Type.INT) //
                .interfaceName(InterfaceName.NumberOfEmployees) //
                .approvedUsage(ModelingMetadata.MODEL_AND_ALL_INSIGHTS_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_NUMERIC) //
                .statisticalType(ModelingMetadata.RATIO_STAT_TYPE) //
                .category(ModelingMetadata.CATEGORY_LEAD_INFORMATION) //
                .build());
        table.addAttribute(attr("Industry") //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "INDUSTRY" })) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.Industry) //
                .approvedUsage(ModelingMetadata.MODEL_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .category(ModelingMetadata.CATEGORY_LEAD_INFORMATION) //
                .build());
        return table;
    }

    private Table getAccountSchema() {
        Table table = createTable(SchemaInterpretation.Account);
        table.setLastModifiedKey(createLastModifiedKey("LastModifiedDate"));
        table.setPrimaryKey(createPrimaryKey("Id"));

        table.addAttribute(attr(InterfaceName.Id.name()) //
                .allowedDisplayNames(Sets.newHashSet("ID", "ACCOUNT", "ACCOUNT ID", "ACCOUNTID", "EXTERNAL_ID")) //
                .type(Schema.Type.STRING) //
                .required() //
                .interfaceName(InterfaceName.AccountId) //
                .logicalType(LogicalDataType.Id) //
                .fundamentalType(FundamentalType.ALPHA.name()) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());
        table.addAttribute(attr(InterfaceName.Industry.name()) //
                .allowedDisplayNames(Sets.newHashSet("INDUSTRY")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.Industry) //
                .approvedUsage(ModelingMetadata.MODEL_APPROVED_USAGE) //
                .fundamentalType(FundamentalType.ALPHA.name()) //
                .subcategory(ModelingMetadata.CATEGORY_LEAD_INFORMATION) //
                .build());
        table.addAttribute(attr(InterfaceName.AnnualRevenue.name()) //
                .allowedDisplayNames(Sets.newHashSet("ANNUALREVENUE", "ANNUAL REVENUE")) //
                .type(Schema.Type.DOUBLE) //
                .interfaceName(InterfaceName.AnnualRevenue) //
                .approvedUsage(ModelingMetadata.MODEL_AND_ALL_INSIGHTS_APPROVED_USAGE) //
                .fundamentalType(FundamentalType.NUMERIC.name()) //
                .statisticalType(ModelingMetadata.RATIO_STAT_TYPE) //
                .subcategory(ModelingMetadata.CATEGORY_LEAD_INFORMATION) //
                .build());
        table.addAttribute(attr(InterfaceName.NumberOfEmployees.name()) //
                .allowedDisplayNames(Sets.newHashSet("NUMBEROFEMPLOYEES", "NUMBER OF EMPLOYEES", "EMPLOYEES")) //
                .type(Schema.Type.INT) //
                .interfaceName(InterfaceName.NumberOfEmployees) //
                .approvedUsage(ModelingMetadata.MODEL_AND_ALL_INSIGHTS_APPROVED_USAGE) //
                .fundamentalType(FundamentalType.NUMERIC.name()) //
                .statisticalType(ModelingMetadata.RATIO_STAT_TYPE) //
                .subcategory(ModelingMetadata.CATEGORY_LEAD_INFORMATION) //
                .build());
        table.addAttribute(attr(InterfaceName.CreatedDate.name()) //
                .allowedDisplayNames(Sets.newHashSet("CREATEDDATE", "CREATED DATE")) //
                .type(Schema.Type.LONG) //
                .interfaceName(InterfaceName.CreatedDate) //
                .logicalType(LogicalDataType.Date) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(FundamentalType.YEAR.name()) //
                .subcategory(ModelingMetadata.CATEGORY_LEAD_INFORMATION) //
                .build());
        table.addAttribute(attr(InterfaceName.LastModifiedDate.name()) //
                .allowedDisplayNames(Sets.newHashSet("LASTMODIFIEDDATE", "LAST MODIFIED DATE", "LASTMODIFIED")) //
                .type(Schema.Type.LONG) //
                .interfaceName(InterfaceName.LastModifiedDate) //
                .logicalType(LogicalDataType.Date) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(FundamentalType.YEAR.name()) //
                .subcategory(ModelingMetadata.CATEGORY_LEAD_INFORMATION) //
                .build());
        table.addAttribute(attr(InterfaceName.YearStarted.name()) //
                .allowedDisplayNames(Sets.newHashSet("YEARSTARTED", "YEAR STARTED")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.YearStarted) //
                .approvedUsage(ModelingMetadata.MODEL_APPROVED_USAGE) //
                .fundamentalType(FundamentalType.YEAR.name()) //
                .statisticalType(ModelingMetadata.NOMINAL_STAT_TYPE) //
                .subcategory(ModelingMetadata.CATEGORY_LEAD_INFORMATION) //
                .build());
        table.addAttribute(attr(InterfaceName.PhoneNumber.name()) //
                .allowedDisplayNames(Sets.newHashSet("PHONE", "PHONE NUMBER")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.PhoneNumber) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(FundamentalType.ALPHA.name()) //
                .subcategory(ModelingMetadata.CATEGORY_LEAD_INFORMATION) //
                .build());
        table.addAttribute(attr(InterfaceName.SalesforceAccountID.name()) //
                .allowedDisplayNames(Sets.newHashSet("CRMACCOUNT_EXTERNAL_ID")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.SalesforceAccountID) //
                .logicalType(LogicalDataType.Id) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());

        return table;
    }

    private Table getContactSchema() {
        Table table = createTable(SchemaInterpretation.Contact);
        table.setLastModifiedKey(createLastModifiedKey("LastModifiedDate"));
        table.setPrimaryKey(createPrimaryKey("Id"));

        table.addAttribute(attr(InterfaceName.Id.name()) //
                .allowedDisplayNames(Sets.newHashSet("ID", "CONTACT", "EXTERNAL_ID", "CONTACT ID")) //
                .type(Schema.Type.STRING) //
                .required() //
                .interfaceName(InterfaceName.ContactId) //
                .logicalType(LogicalDataType.Id) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr(InterfaceName.ContactName.name()) //
                .allowedDisplayNames(Sets.newHashSet("NAME", "CONTACT NAME", "CONTACT_NAME", "DISPLAY_NAME")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.ContactName) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .subcategory(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());
        table.addAttribute(attr(InterfaceName.AccountId.name()) //
                .allowedDisplayNames(
                        Sets.newHashSet("ACCOUNT_ID", "ACCOUNTID", "ACCOUNT_EXTERNAL_ID", "ACCOUNT ID", "ACCOUNT")) //
                .type(Schema.Type.STRING) //
                .required() //
                .interfaceName(InterfaceName.AccountId) //
                .logicalType(LogicalDataType.Id) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr(InterfaceName.Title.name()) //
                .allowedDisplayNames(Sets.newHashSet("TITLE")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.Title) //
                .approvedUsage(ModelingMetadata.MODEL_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .subcategory(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());
        table.addAttribute(attr(InterfaceName.CreatedDate.name()) //
                .allowedDisplayNames(Sets.newHashSet("CREATEDDATE", "CREATED DATE")) //
                .type(Schema.Type.LONG) //
                .interfaceName(InterfaceName.CreatedDate) //
                .logicalType(LogicalDataType.Date) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_YEAR) //
                .subcategory(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());
        table.addAttribute(attr(InterfaceName.LastModifiedDate.name()) //
                .allowedDisplayNames(Sets.newHashSet("LASTMODIFIEDDATE", "LAST MODIFIED DATE", "LASTMODIFIED")) //
                .type(Schema.Type.LONG) //
                .interfaceName(InterfaceName.LastModifiedDate) //
                .logicalType(LogicalDataType.Date) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_YEAR) //
                .subcategory(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());
        table.addAttribute(attr(InterfaceName.Salutation.name()) //
                .allowedDisplayNames(Sets.newHashSet("SALUTATION")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.Salutation) //
                .approvedUsage(ModelingMetadata.MODEL_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .subcategory(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());
        table.addAttribute(attr(InterfaceName.BirthDate.name()) //
                .allowedDisplayNames(Sets.newHashSet("BIRTHDATE", "BIRTH DATE")) //
                .type(Schema.Type.LONG) //
                .interfaceName(InterfaceName.BirthDate) //
                .logicalType(LogicalDataType.Date) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_YEAR) //
                .subcategory(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());
        table.addAttribute(attr(InterfaceName.LeadSource.name()) //
                .allowedDisplayNames(Sets.newHashSet("LEADSOURCE", "LEAD SOURCE")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.LeadSource) //
                .approvedUsage(ModelingMetadata.MODEL_AND_ALL_INSIGHTS_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .statisticalType(ModelingMetadata.NOMINAL_STAT_TYPE) //
                .subcategory(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());
        table.addAttribute(attr(InterfaceName.AnnualRevenue.name()) //
                .allowedDisplayNames(Sets.newHashSet("ANNUALREVENUE", "ANNUAL REVENUE")) //
                .type(Schema.Type.DOUBLE) //
                .interfaceName(InterfaceName.AnnualRevenue) //
                .approvedUsage(ModelingMetadata.MODEL_AND_ALL_INSIGHTS_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_NUMERIC) //
                .statisticalType(ModelingMetadata.RATIO_STAT_TYPE) //
                .subcategory(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());
        table.addAttribute(attr(InterfaceName.NumberOfEmployees.name()) //
                .allowedDisplayNames(Sets.newHashSet("NUMBEROFEMPLOYEES", "NUMBER OF EMPLOYEES", "NO. OF EMPLOYEES")) //
                .type(Schema.Type.INT) //
                .interfaceName(InterfaceName.NumberOfEmployees) //
                .approvedUsage(ModelingMetadata.MODEL_AND_ALL_INSIGHTS_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_NUMERIC) //
                .statisticalType(ModelingMetadata.RATIO_STAT_TYPE) //
                .subcategory(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());
        table.addAttribute(attr(InterfaceName.Industry.name()) //
                .allowedDisplayNames(Sets.newHashSet("INDUSTRY")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.Industry) //
                .approvedUsage(ModelingMetadata.MODEL_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .subcategory(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());
        table.addAttribute(attr(InterfaceName.SalesforceContactID.name()) //
                .allowedDisplayNames(Sets.newHashSet("CRM_ID")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.SalesforceAccountID) //
                .logicalType(LogicalDataType.Id) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr(InterfaceName.DoNotMail.name()) //
                .allowedDisplayNames(Sets.newHashSet("DONOTMAIL")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.DoNotMail) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(FundamentalType.BOOLEAN.name()) //
                .build());
        return table;
    }

    private Table getProductSchema() {
        Table table = createTable(SchemaInterpretation.Product);
        table.setLastModifiedKey(createLastModifiedKey("LastModifiedDate"));
        table.setPrimaryKey(createPrimaryKey("Id"));

        table.addAttribute(attr(InterfaceName.Id.name()) //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "ID", "PRODUCT_ID", "PRODUCT ID" })) //
                .type(Schema.Type.STRING) //
                .required() //
                .interfaceName(InterfaceName.ProductId) //
                .logicalType(LogicalDataType.Id) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr(InterfaceName.ProductName.name()) //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "NAME", "PRODUCT_NAME", "PRODUCT NAME" })) //
                .type(Schema.Type.STRING) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .statisticalType(ModelingMetadata.NOMINAL_STAT_TYPE) //
                .build());
        table.addAttribute(attr("Description") //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "DESCRIPTION" })) //
                .type(Schema.Type.STRING) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .statisticalType(ModelingMetadata.NOMINAL_STAT_TYPE) //
                .build());
        table.addAttribute(attr("Category") //
                .allowedDisplayNames(
                        Sets.newHashSet(new String[] { "CATEGORY", "PRODUCT_CATEGORY", "PRODUCT CATEGORY" })) //
                .type(Schema.Type.STRING) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .statisticalType(ModelingMetadata.NOMINAL_STAT_TYPE) //
                .build());
        return table;
    }

    private Table getTransactionSchema() {
        Table table = createTable(SchemaInterpretation.Transaction);
        table.setPrimaryKey(createPrimaryKey("Id"));

        table.addAttribute(attr(InterfaceName.Id.name()) //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "ID", "TRANSACTION_ID", "TRANSACTION ID" })) //
                .type(Schema.Type.STRING) //
                .required() //
                .interfaceName(InterfaceName.TransactionId) //
                .logicalType(LogicalDataType.Id) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr(InterfaceName.AccountId.name()) //
                .allowedDisplayNames(Sets.newHashSet(
                        new String[] { "ACCOUNT_ID", "ACCOUNTID", "ACCOUNT_EXTERNAL_ID", "ACCOUNT ID", "ACCOUNT" })) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.AccountId) //
                .logicalType(LogicalDataType.Id) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr(InterfaceName.ContactId.name()) //
                .allowedDisplayNames(Sets.newHashSet(
                        new String[] { "CONTACT_ID", "CONTACTID", "CONTACT_EXTERNAL_ID", "CONTACT ID", "CONTACT" })) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.ContactId) //
                .logicalType(LogicalDataType.Id) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr(InterfaceName.ProductId.name()) //
                .allowedDisplayNames(Sets
                        .newHashSet(new String[] { "PRODUCT_ID", "PRODUCTID", "PRODUCT_EXTERNAL_ID", "PRODUCT ID" })) //
                .type(Schema.Type.STRING) //
                .required() //
                .interfaceName(InterfaceName.ProductId) //
                .logicalType(LogicalDataType.Id) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr(InterfaceName.OrderId.name()) //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "ORDER_ID", "ORDERID", "ORDER ID" })) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.OrderId) //
                .logicalType(LogicalDataType.Id) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr(InterfaceName.LastModifiedDate.name()) //
                .allowedDisplayNames(
                        Sets.newHashSet(new String[] { "LASTMODIFIEDDATE", "LAST MODIFIED DATE", "LASTMODIFIED" })) //
                .type(Schema.Type.LONG) //
                .interfaceName(InterfaceName.LastModifiedDate) //
                .logicalType(LogicalDataType.Timestamp) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_YEAR) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());
        table.addAttribute(attr(InterfaceName.Quantity.name()) //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "QUANTITY" })) //
                .type(Schema.Type.LONG) //
                .interfaceName(InterfaceName.Quantity) //
                .logicalType(LogicalDataType.Metric) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_NUMERIC) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());
        table.addAttribute(attr(InterfaceName.Amount.name()) //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "AMOUNT" })) //
                .type(Schema.Type.LONG) //
                .interfaceName(InterfaceName.Amount) //
                .logicalType(LogicalDataType.Metric) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_NUMERIC) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());
        table.addAttribute(attr(InterfaceName.TransactionTime.name()) //
                .allowedDisplayNames(Sets
                        .newHashSet(new String[] { "TIMESTAMP", "TIME STAMP", "TRANSACTION_TIME", "TRANSACTION TIME" })) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.TransactionTime) //
                .logicalType(LogicalDataType.Timestamp) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_NUMERIC) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());
        table.addAttribute(attr(InterfaceName.TransactionType.name()) //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "TYPE", "TRANSACTION_TYPE", "TRANSACTION TYPE" })) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.TransactionType) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());
        return table;
    }

    private Table getTransactionDailyAggregationSchema() {
        Table table = createTable(SchemaInterpretation.TransactionDailyAggregation);
        table.addAttribute(attr(InterfaceName.AccountId.name()) //
                .allowedDisplayNames(Sets.newHashSet(
                        new String[] { "ACCOUNT_ID", "ACCOUNTID", "ACCOUNT_EXTERNAL_ID", "ACCOUNT ID", "ACCOUNT" })) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.AccountId) //
                .logicalType(LogicalDataType.Id) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr(InterfaceName.ContactId.name()) //
                .allowedDisplayNames(Sets.newHashSet(
                        new String[] { "CONTACT_ID", "CONTACTID", "CONTACT_EXTERNAL_ID", "CONTACT ID", "CONTACT" })) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.ContactId) //
                .logicalType(LogicalDataType.Id) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr(InterfaceName.Id.name()) //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "ID", "PRODUCT_ID", "PRODUCT ID" })) //
                .type(Schema.Type.STRING) //
                .required() //
                .interfaceName(InterfaceName.ProductId) //
                .logicalType(LogicalDataType.Id) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr(InterfaceName.TransactionType.name()) //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "TYPE", "TRANSACTION_TYPE", "TRANSACTION TYPE" })) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.TransactionType) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr(InterfaceName.TransactionDate.name()) //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "DATE", "TRANSACTION_DATE", "TRANSACTION DATE" })) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.TransactionDate) //
                .logicalType(LogicalDataType.Date) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr(InterfaceName.TotalAmount.name()) //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "AMOUNT", "TOTAL AMOUNT" })) //
                .type(Schema.Type.DOUBLE) //
                .required() //
                .interfaceName(InterfaceName.TotalAmount) //
                .logicalType(LogicalDataType.Metric) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_NUMERIC) //
                .build());
        table.addAttribute(attr(InterfaceName.TotalQuantity.name()) //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "QUANTITY", "TOTAL QUANTITY" })) //
                .type(Schema.Type.LONG) //
                .required() //
                .interfaceName(InterfaceName.TotalQuantity) //
                .logicalType(LogicalDataType.Metric) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_NUMERIC) //
                .build());
        return table;
    }

    private LastModifiedKey createLastModifiedKey(String columnName) {
        LastModifiedKey lmk = new LastModifiedKey();
        lmk.setDisplayName(columnName);
        lmk.setName(columnName);
        lmk.setLastModifiedTimestamp(DateTime.now().getMillis());
        lmk.setAttributes(Arrays.asList(columnName));
        return lmk;
    }

    private PrimaryKey createPrimaryKey(String... columnList) {
        PrimaryKey pk = new PrimaryKey();
        pk.setDisplayName("PK");
        pk.setName("PK");
        pk.setAttributes(Arrays.asList(columnList));
        return pk;
    }

    private Table createTable(SchemaInterpretation interpretation) {
        Table table = new Table();
        String interpretationString = interpretation.toString();
        table.setInterpretation(interpretationString);
        table.setName(interpretationString);
        table.setDisplayName(interpretationString);
        return table;
    }

    private AttributeBuilder attr(String name) {
        AttributeBuilder builder = new AttributeBuilder();
        builder.name(name);
        builder.tag(Tag.INTERNAL.toString());
        return builder;
    }

    public List<Attribute> matchingAttributes(SchemaInterpretation schema) {
        Attribute website = attr("Website") //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "WEBSITE" })) //
                .type(Schema.Type.STRING) //
                .required() //
                .interfaceName(InterfaceName.Website) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .statisticalType(ModelingMetadata.NOMINAL_STAT_TYPE) //
                .build();

        Attribute email = attr("Email") //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "EMAIL" })) //
                .type(Schema.Type.STRING) //
                .required() //
                .interfaceName(InterfaceName.Email) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_EMAIL) //
                .statisticalType(ModelingMetadata.NOMINAL_STAT_TYPE) //
                .category(ModelingMetadata.CATEGORY_LEAD_INFORMATION) //
                .build();

        Attribute companyName = attr("CompanyName") //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "COMPANY NAME", "ACCOUNT NAME" })) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.CompanyName) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build();
        Attribute city = attr("City") //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "CITY", "BILLING CITY" })) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.City) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build();
        Attribute state = attr("State") //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "STATE", "BILLING STATE", "BILLING PROVINCE" })) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.State) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build();
        Attribute country = attr("Country") //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "COUNTRY", "BILLING COUNTRY" })) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.Country) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build();
        Attribute postalCode = attr("PostalCode") //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "POSTALCODE", "BILLING ZIP", "POSTAL CODE" })) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.PostalCode) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_NUMERIC) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build();

        Attribute phoneNumber = attr("PhoneNumber") //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "PHONE", "PHONE NUMBER" })) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.PhoneNumber) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build();

        Attribute duns = attr("DUNS") //
                .allowedDisplayNames(Sets.newHashSet(new String[] { "DUNS", "DUNS NUMBER" })) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.DUNS) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_NUMERIC) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build();
        List<Attribute> attrs = new ArrayList<>();
        if (schema == SchemaInterpretation.Account || schema == SchemaInterpretation.SalesforceAccount) {
            attrs.add(website);
        } else if (schema == SchemaInterpretation.Contact || schema == SchemaInterpretation.SalesforceLead) {
            attrs.add(email);
        }
        attrs.addAll(Arrays.asList(companyName, city, state, country, postalCode, phoneNumber, duns));
        return attrs;
    }

    public List<Attribute> matchingAttributes(BusinessEntity entity) {
        if (entity == BusinessEntity.Contact)
            return matchingAttributes(SchemaInterpretation.Contact);
        if (entity == BusinessEntity.Account)
            return matchingAttributes(SchemaInterpretation.Account);
        return Collections.emptyList();
    }

    private static class AttributeBuilder {
        private Attribute attribute = new Attribute();

        public AttributeBuilder() {
            attribute.setNullable(true);
            attribute.setApprovedUsage(ModelingMetadata.MODEL_AND_ALL_INSIGHTS_APPROVED_USAGE);
        }

        public Attribute build() {
            return attribute;
        }

        public AttributeBuilder name(String name) {
            attribute.setName(name);
            attribute.setDisplayName(name);
            return this;
        }

        public AttributeBuilder type(Schema.Type type) {
            attribute.setPhysicalDataType(type.toString());
            return this;
        }

        public AttributeBuilder interfaceName(InterfaceName interfaceName) {
            attribute.setInterfaceName(interfaceName);
            return this;
        }

        @SuppressWarnings("unused")
        public AttributeBuilder withValidator(String otherField) {
            attribute.addValidator(new RequiredIfOtherFieldIsEmpty(otherField));
            return this;
        }

        public AttributeBuilder required() {
            attribute.setNullable(false);
            return this;
        }

        public AttributeBuilder approvedUsage(String approvedUsage) {
            attribute.setApprovedUsage(approvedUsage);
            return this;
        }

        public AttributeBuilder logicalType(LogicalDataType logicalDataType) {
            attribute.setLogicalDataType(logicalDataType);
            return this;
        }

        public AttributeBuilder allowedDisplayNames(Set<String> allowedDisplayNames) {
            List<String> list = new ArrayList<>();
            list.addAll(allowedDisplayNames);
            attribute.setAllowedDisplayNames(list);
            return this;
        }

        public AttributeBuilder fundamentalType(String fundamentalType) {
            attribute.setFundamentalType(fundamentalType);
            return this;
        }

        public AttributeBuilder category(String category) {
            attribute.setCategory(category);
            return this;
        }

        public AttributeBuilder subcategory(String subcategory) {
            attribute.setSubcategory(subcategory);
            return this;
        }

        public AttributeBuilder statisticalType(String statisticalType) {
            attribute.setStatisticalType(statisticalType);
            return this;
        }

        public AttributeBuilder tag(String tag) {
            attribute.setTags(tag);
            return this;
        }
    }

}
