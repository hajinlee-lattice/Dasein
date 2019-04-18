package com.latticeengines.domain.exposed.metadata.standardschemas;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.joda.time.DateTime;

import com.google.common.collect.Sets;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LastModifiedKey;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.PrimaryKey;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.metadata.validators.FailImportIfFieldIsEmpty;
import com.latticeengines.domain.exposed.metadata.validators.RequiredIfOtherFieldIsEmpty;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExportType;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class SchemaRepository {
    private static SchemaRepository instance;

    private SchemaRepository() {
    }

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

    public static Set<InterfaceName> getSystemAttributes(BusinessEntity entity) {
        Set<InterfaceName> sysAttrs = new HashSet<>();
        if (BusinessEntity.LatticeAccount.equals(entity)) {
            sysAttrs.add(InterfaceName.LatticeAccountId);
            sysAttrs.add(InterfaceName.IsMatched);
        } else if (BusinessEntity.AnalyticPurchaseState.equals(entity)) {
            sysAttrs.add(InterfaceName.LEAccount_ID);
            sysAttrs.add(InterfaceName.Period_ID);
            sysAttrs.add(InterfaceName.AnalyticPurchaseState_ID);
        } else {
            // common
            if (!BusinessEntity.Account.equals(entity)) {
                sysAttrs.add(InterfaceName.AccountId);
            }
            sysAttrs.add(InterfaceName.EntityId);
            sysAttrs.add(InterfaceName.InternalId);
            sysAttrs.add(InterfaceName.CDLCreatedTime);
            sysAttrs.add(InterfaceName.CDLUpdatedTime);
            // special
            if (BusinessEntity.Account.equals(entity)) {
                sysAttrs.add(InterfaceName.LatticeAccountId);
                sysAttrs.add(InterfaceName.CustomerParentAccountID);
            }
        }
        return sysAttrs;
    }

    public static Set<InterfaceName> getStandardAttributes(BusinessEntity entity) {
        Set<InterfaceName> stdAttrs = new HashSet<>();
        // only account and contact has standard attrs
        if (BusinessEntity.Account.equals(entity) || BusinessEntity.Contact.equals(entity)) {
            // common
            stdAttrs.add(InterfaceName.City);
            stdAttrs.add(InterfaceName.State);
            stdAttrs.add(InterfaceName.Country);
            stdAttrs.add(InterfaceName.PostalCode);
            stdAttrs.add(InterfaceName.PhoneNumber);
            stdAttrs.add(InterfaceName.CompanyName);
            stdAttrs.add(InterfaceName.DUNS);
            stdAttrs.add(InterfaceName.Address_Street_1);
            stdAttrs.add(InterfaceName.Address_Street_2);
            // special
            switch (entity) {
            case Account:
                stdAttrs.add(InterfaceName.AccountId);
                stdAttrs.add(InterfaceName.Website);
                stdAttrs.add(InterfaceName.IsMatched);
                break;
            case Contact:
                stdAttrs.add(InterfaceName.ContactName);
                stdAttrs.add(InterfaceName.ContactId);
                stdAttrs.add(InterfaceName.Email);
                break;
            default:
            }
        }
        return stdAttrs;
    }

    public static Set<InterfaceName> getDefaultExportAttributes(BusinessEntity entity) {
        return MetadataSegmentExportType.getDefaultExportAttributes(entity);
    }

    public Table getSchema(BusinessEntity entity, boolean cdlSchema, boolean withoutId, boolean enableEntityMatch) {
        Table table = null;
        switch (entity) {
        case Account:
            table = getAccountSchema(cdlSchema, enableEntityMatch);
            break;
        case Contact:
            table = getContactSchema(cdlSchema, enableEntityMatch);
            break;
        case Product:
            table = getProductSchema();
            break;
        case Transaction:
            table = getTransactionSchema();
            break;
        case PeriodTransaction:
            table = getAggregatedTransactionSchema(SchemaInterpretation.TransactionPeriodAggregation, false);
            break;
        default:
            throw new RuntimeException(String.format("Unsupported schema %s", entity));
        }

        table.addAttributes(matchingAttributes(entity, enableEntityMatch));

        return table;
    }

    public Table getSchema(BusinessEntity entity) {
        return getSchema(entity, true, false, false);
    }

    public Table getSchema(SchemaInterpretation schema) {
        return getSchema(schema, false, false, false);
    }

    public Table getSchema(SchemaInterpretation schema, boolean withoutId) {
        return getSchema(schema, false, withoutId, false);
    }

    public Table getSchema(SchemaInterpretation schema, boolean withoutId, boolean enableEntityMatch) {
        return getSchema(schema, false, withoutId, enableEntityMatch);
    }

    public Table getSchema(SchemaInterpretation schema, boolean includeCdlTimestamps, boolean withoutId,
            boolean enableEntityMatch) {
        Table table = null;
        switch (schema) {
        case SalesforceAccount:
            table = getSalesforceAccountSchema();
            break;
        case SalesforceLead:
            table = getSalesforceLeadSchema();
            break;
        case Account:
            table = getAccountSchema(false, enableEntityMatch);
            break;
        case Contact:
            table = getContactSchema(false, enableEntityMatch);
            break;
        case Product:
            table = getProductSchema();
            break;
        case TimeSeries:
            table = getTimeSeriesSchema();
            break;
        case Category:
            table = getCategorySchema();
            break;
        case Transaction:
            table = getTransactionSchema();
            break;
        case TransactionRaw:
            table = getRawTransactionSchema(includeCdlTimestamps);
            break;
        case TransactionDailyAggregation:
            table = getAggregatedTransactionSchema(SchemaInterpretation.TransactionDailyAggregation,
                    includeCdlTimestamps);
            break;
        case TransactionPeriodAggregation:
            table = getAggregatedTransactionSchema(SchemaInterpretation.TransactionPeriodAggregation,
                    includeCdlTimestamps);
            break;
        case DeleteAccountTemplate:
            table = getDeleteAccountTemplateSchema();
            break;
        case DeleteContactTemplate:
            table = getDeleteContactTemplateSchema();
            break;
        case DeleteTransactionTemplate:
            table = getDeleteTransactionTemplateSchema();
            break;
        default:
            throw new RuntimeException(String.format("Unsupported schema %s", schema));
        }

        if (enableEntityMatch && schema == SchemaInterpretation.Contact) {
            table.addAttributes(getMatchingAttributes(SchemaInterpretation.ContactEntityMatch));
        } else {
            table.addAttributes(getMatchingAttributes(schema));
        }
        return table;
    }

    private Table getCategorySchema() {
        Table table = createTable(SchemaInterpretation.Category);
        table.setLastModifiedKey(createLastModifiedKey("LastModifiedDate"));
        table.setPrimaryKey(createPrimaryKey("Id"));

        table.addAttribute(attr("CategoryId") //
                .allowedDisplayNames(Sets.newHashSet("CATEGORY", "CATEGORY ID")) //
                .type(Schema.Type.STRING) //
                .notNull() //
                .interfaceName(InterfaceName.Id) //
                .logicalType(LogicalDataType.Reference) //
                .fundamentalType(FundamentalType.ALPHA.name()) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());
        table.addAttribute(attr("SubcategoryId") //
                .allowedDisplayNames(Sets.newHashSet("SUBCATEGORY", "SUBCATEGORY ID")) //
                .type(Schema.Type.STRING) //
                .notNull() //
                .interfaceName(InterfaceName.Id) //
                .logicalType(LogicalDataType.Id) //
                .fundamentalType(FundamentalType.ALPHA.name()) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());
        table.addAttribute(attr("Description") //
                .allowedDisplayNames(Sets.newHashSet("DESCRIPTION")) //
                .type(Schema.Type.STRING) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .category(ModelingMetadata.CATEGORY_LEAD_INFORMATION) //
                .statisticalType(ModelingMetadata.NOMINAL_STAT_TYPE) //
                .build());
        table.addAttribute(attr("LastModifiedDate") //
                .allowedDisplayNames(Sets.newHashSet("LASTMODIFIEDDATE", "LAST MODIFIED DATE")) //
                .type(Schema.Type.LONG) //
                .notNull() //
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
                .allowedDisplayNames(Sets.newHashSet("ACCOUNT_ID", "ACCOUNTID")) //
                .type(Schema.Type.STRING) //
                .notNull() //
                .interfaceName(InterfaceName.AccountId) //
                .logicalType(LogicalDataType.Id) //
                .fundamentalType(FundamentalType.ALPHA.name()) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());
        table.addAttribute(attr("SubcategoryId") //
                .allowedDisplayNames(Sets.newHashSet("SUBCATEGORY_ID")) //
                .type(Schema.Type.STRING) //
                .notNull() //
                .interfaceName(InterfaceName.SubcategoryId) //
                .logicalType(LogicalDataType.Reference) //
                .fundamentalType(FundamentalType.ALPHA.name()) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());
        table.addAttribute(attr("Timestamp") //
                .allowedDisplayNames(Sets.newHashSet("TIMESTAMP", "TIME STAMP")) //
                .type(Schema.Type.LONG) //
                .notNull() //
                .interfaceName(InterfaceName.LastModifiedDate) //
                .logicalType(LogicalDataType.Timestamp) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_YEAR) //
                .category(ModelingMetadata.CATEGORY_LEAD_INFORMATION) //
                .build());
        table.addAttribute(attr("Quantity") //
                .allowedDisplayNames(Sets.newHashSet("QUANTITY")) //
                .type(Schema.Type.LONG) //
                .notNull() //
                .interfaceName(InterfaceName.Quantity) //
                .logicalType(LogicalDataType.Metric) //
                .approvedUsage(ModelingMetadata.MODEL_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_NUMERIC) //
                .category(ModelingMetadata.CATEGORY_LEAD_INFORMATION) //
                .build());
        table.addAttribute(attr("Amount") //
                .allowedDisplayNames(Sets.newHashSet("AMOUNT")) //
                .type(Schema.Type.LONG) //
                .notNull() //
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
        table.setPrimaryKey(createPrimaryKey(InterfaceName.Id.name()));

        table.addAttribute(attr(InterfaceName.Id.name()) //
                .allowedDisplayNames(Sets.newHashSet("ID", "ACCOUNT", "ACCOUNT ID")) //
                .type(Schema.Type.STRING) //
                .notNull() //
                .interfaceName(InterfaceName.Id) //
                .logicalType(LogicalDataType.Id) //
                .fundamentalType(FundamentalType.ALPHA.name()) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());

        table.addAttribute(attr("Event") //
                .allowedDisplayNames(Sets.newHashSet("EVENT", "WON", "P1_EVENT")) //
                .type(Schema.Type.BOOLEAN) //
                .notNull() //
                .interfaceName(InterfaceName.Event) //
                .logicalType(LogicalDataType.Event) //
                .fundamentalType(ModelingMetadata.FT_BOOLEAN) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());

        table.addAttribute(attr("Industry") //
                .allowedDisplayNames(Sets.newHashSet("INDUSTRY")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.Industry) //
                .approvedUsage(ModelingMetadata.MODEL_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());
        table.addAttribute(attr("AnnualRevenue") //
                .allowedDisplayNames(Sets.newHashSet("ANNUALREVENUE", "ANNUAL REVENUE")) //
                .type(Schema.Type.DOUBLE) //
                .interfaceName(InterfaceName.AnnualRevenue) //
                .approvedUsage(ModelingMetadata.MODEL_AND_ALL_INSIGHTS_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_NUMERIC) //
                .statisticalType(ModelingMetadata.RATIO_STAT_TYPE) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());
        table.addAttribute(attr("NumberOfEmployees") //
                .allowedDisplayNames(Sets.newHashSet("NUMBEROFEMPLOYEES", "NUMBER OF EMPLOYEES", "EMPLOYEES")) //
                .type(Schema.Type.INT) //
                .interfaceName(InterfaceName.NumberOfEmployees) //
                .approvedUsage(ModelingMetadata.MODEL_AND_ALL_INSIGHTS_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_NUMERIC) //
                .statisticalType(ModelingMetadata.RATIO_STAT_TYPE) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());
        table.addAttribute(attr("CreatedDate") //
                .allowedDisplayNames(Sets.newHashSet("CREATEDDATE", "CREATED DATE")) //
                .type(Schema.Type.LONG) //
                .interfaceName(InterfaceName.CreatedDate) //
                .logicalType(LogicalDataType.Date) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_YEAR) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());
        table.addAttribute(attr("LastModifiedDate") //
                .allowedDisplayNames(Sets.newHashSet("LASTMODIFIEDDATE", "LAST MODIFIED DATE")) //
                .type(Schema.Type.LONG) //
                .interfaceName(InterfaceName.LastModifiedDate) //
                .logicalType(LogicalDataType.Date) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_YEAR) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());
        table.addAttribute(attr("YearStarted") //
                .allowedDisplayNames(Sets.newHashSet("YEARSTARTED", "YEAR STARTED")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.YearStarted) //
                .approvedUsage(ModelingMetadata.MODEL_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_YEAR) //
                .statisticalType(ModelingMetadata.NOMINAL_STAT_TYPE) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());
        table.addAttribute(attr("IsClosed") //
                .allowedDisplayNames(Sets.newHashSet("ISCLOSED", "IS CLOSED", "CLOSED")) //
                .type(Schema.Type.BOOLEAN) //
                .interfaceName(InterfaceName.IsClosed) //
                .logicalType(LogicalDataType.Opportunity) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_BOOLEAN) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());
        table.addAttribute(attr("StageName") //
                .allowedDisplayNames(Sets.newHashSet("STAGE NAME", "STAGE")) //
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
        table.setPrimaryKey(createPrimaryKey(InterfaceName.Id.name()));

        table.addAttribute(attr(InterfaceName.Id.name()) //
                .allowedDisplayNames(Sets.newHashSet("ID", "LEAD", "LEAD ID")) //
                .type(Schema.Type.STRING) //
                .notNull() //
                .interfaceName(InterfaceName.Id) //
                .logicalType(LogicalDataType.Id) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr("Event") //
                .type(Schema.Type.BOOLEAN) //
                .allowedDisplayNames(Sets.newHashSet("EVENT", "WON", "P1_EVENT")) //
                .notNull()//
                .interfaceName(InterfaceName.Event) //
                .logicalType(LogicalDataType.Event) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_BOOLEAN) //
                .build());
        table.addAttribute(attr("CreatedDate") //
                .allowedDisplayNames(Sets.newHashSet("CREATEDDATE", "CREATED DATE")) //
                .type(Schema.Type.LONG) //
                .interfaceName(InterfaceName.CreatedDate) //
                .logicalType(LogicalDataType.Date) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_YEAR) //
                .category(ModelingMetadata.CATEGORY_LEAD_INFORMATION) //
                .build());
        table.addAttribute(attr("LastModifiedDate") //
                .allowedDisplayNames(Sets.newHashSet("LASTMODIFIEDDATE", "LAST MODIFIED DATE")) //
                .type(Schema.Type.LONG) //
                .interfaceName(InterfaceName.LastModifiedDate) //
                .logicalType(LogicalDataType.Date) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_YEAR) //
                .category(ModelingMetadata.CATEGORY_LEAD_INFORMATION) //
                .build());
        table.addAttribute(attr("FirstName") //
                .allowedDisplayNames(Sets.newHashSet("FIRSTNAME", "FIRST NAME")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.FirstName) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .category(ModelingMetadata.CATEGORY_LEAD_INFORMATION) //
                .build());
        table.addAttribute(attr("LastName") //
                .allowedDisplayNames(Sets.newHashSet("LASTNAME", "LAST NAME")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.LastName) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .category(ModelingMetadata.CATEGORY_LEAD_INFORMATION) //
                .build());
        table.addAttribute(attr("Title") //
                .allowedDisplayNames(Sets.newHashSet("TITLE")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.Title) //
                .approvedUsage(ModelingMetadata.MODEL_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .category(ModelingMetadata.CATEGORY_LEAD_INFORMATION) //
                .build());
        table.addAttribute(attr("LeadSource") //
                .allowedDisplayNames(Sets.newHashSet("LEADSOURCE", "LEAD SOURCE")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.LeadSource) //
                .approvedUsage(ModelingMetadata.MODEL_AND_ALL_INSIGHTS_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .statisticalType(ModelingMetadata.NOMINAL_STAT_TYPE) //
                .category(ModelingMetadata.CATEGORY_LEAD_INFORMATION) //
                .build());
        table.addAttribute(attr("IsClosed") //
                .allowedDisplayNames(Sets.newHashSet("ISCLOSED", "IS CLOSED", "CLOSED")) //
                .type(Schema.Type.BOOLEAN) //
                .interfaceName(InterfaceName.IsClosed) //
                .logicalType(LogicalDataType.Opportunity) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_BOOLEAN) //
                .category(ModelingMetadata.CATEGORY_LEAD_INFORMATION) //
                .build());
        table.addAttribute(attr("StageName") //
                .allowedDisplayNames(Sets.newHashSet("STAGE NAME", "STAGE")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.StageName) //
                .logicalType(LogicalDataType.Opportunity) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .category(ModelingMetadata.CATEGORY_LEAD_INFORMATION) //
                .build());
        table.addAttribute(attr("AnnualRevenue") //
                .allowedDisplayNames(Sets.newHashSet("ANNUALREVENUE", "ANNUAL REVENUE")) //
                .type(Schema.Type.DOUBLE) //
                .interfaceName(InterfaceName.AnnualRevenue) //
                .approvedUsage(ModelingMetadata.MODEL_AND_ALL_INSIGHTS_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_NUMERIC) //
                .statisticalType(ModelingMetadata.RATIO_STAT_TYPE) //
                .category(ModelingMetadata.CATEGORY_LEAD_INFORMATION) //
                .build());
        table.addAttribute(attr("NumberOfEmployees") //
                .allowedDisplayNames(Sets.newHashSet("NUMBEROFEMPLOYEES", "NUMBER OF EMPLOYEES", "NO. OF EMPLOYEES")) //
                .type(Schema.Type.INT) //
                .interfaceName(InterfaceName.NumberOfEmployees) //
                .approvedUsage(ModelingMetadata.MODEL_AND_ALL_INSIGHTS_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_NUMERIC) //
                .statisticalType(ModelingMetadata.RATIO_STAT_TYPE) //
                .category(ModelingMetadata.CATEGORY_LEAD_INFORMATION) //
                .build());
        table.addAttribute(attr("Industry") //
                .allowedDisplayNames(Sets.newHashSet("INDUSTRY")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.Industry) //
                .approvedUsage(ModelingMetadata.MODEL_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .category(ModelingMetadata.CATEGORY_LEAD_INFORMATION) //
                .build());
        return table;
    }

    private Table getAccountSchema() {
        return getAccountSchema(false, false);
    }

    private Table getAccountSchema(boolean cdlSchema, boolean enableEntityMatch) {
        Table table = createTable(SchemaInterpretation.Account);
        if (!enableEntityMatch) {
            table.setPrimaryKey(createPrimaryKey(InterfaceName.AccountId.name()));
        }

        if (enableEntityMatch) {
            table.addAttribute(attr(InterfaceName.CustomerAccountId.name()) //
                    .allowedDisplayNames(Sets.newHashSet("ID", "ACCOUNT", "ACCOUNT ID", "ACCOUNTID", "EXTERNAL_ID")) //
                    .type(Schema.Type.STRING) //
                    .interfaceName(InterfaceName.CustomerAccountId) //
                    .logicalType(LogicalDataType.Id) //
                    .fundamentalType(FundamentalType.ALPHA.name()) //
                    .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                    .build());
        } else {
            table.addAttribute(attr(InterfaceName.AccountId.name()) //
                    .allowedDisplayNames(Sets.newHashSet("ID", "ACCOUNT", "ACCOUNT ID", "ACCOUNTID", "EXTERNAL_ID")) //
                    .type(Schema.Type.STRING) //
                    .notNull() //
                    .required() //
                    .interfaceName(InterfaceName.AccountId) //
                    .logicalType(LogicalDataType.Id) //
                    .fundamentalType(FundamentalType.ALPHA.name()) //
                    .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                    .build());
        }
        table.addAttribute(attr(InterfaceName.Industry.name()) //
                .allowedDisplayNames(Sets.newHashSet("INDUSTRY")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.Industry) //
                .approvedUsage(ModelingMetadata.MODEL_APPROVED_USAGE) //
                .fundamentalType(FundamentalType.ALPHA.name()) //
                .build());
        table.addAttribute(attr(InterfaceName.AnnualRevenue.name()) //
                .allowedDisplayNames(Sets.newHashSet("ANNUALREVENUE", "ANNUAL REVENUE")) //
                .type(Schema.Type.DOUBLE) //
                .interfaceName(InterfaceName.AnnualRevenue) //
                .approvedUsage(ModelingMetadata.MODEL_AND_ALL_INSIGHTS_APPROVED_USAGE) //
                .fundamentalType(FundamentalType.NUMERIC.name()) //
                .statisticalType(ModelingMetadata.RATIO_STAT_TYPE) //
                .build());
        table.addAttribute(attr(InterfaceName.NumberOfEmployees.name()) //
                .allowedDisplayNames(Sets.newHashSet("NUMBEROFEMPLOYEES", "NUMBER OF EMPLOYEES", "EMPLOYEES")) //
                .type(Schema.Type.INT) //
                .interfaceName(InterfaceName.NumberOfEmployees) //
                .approvedUsage(ModelingMetadata.MODEL_AND_ALL_INSIGHTS_APPROVED_USAGE) //
                .fundamentalType(FundamentalType.NUMERIC.name()) //
                .statisticalType(ModelingMetadata.RATIO_STAT_TYPE) //
                .build());
        table.addAttribute(attr(InterfaceName.Type.name()) //
                .allowedDisplayNames(Sets.newHashSet("TYPE")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.Type) //
                .fundamentalType(FundamentalType.ALPHA.name()) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());
        table.addAttribute(attr(InterfaceName.AnnualRevenueCurrency.name()) //
                .allowedDisplayNames(Sets.newHashSet("ANNUALREVENUECURRENCY", "ANNUAL_REVENUE_CURRENCY")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.AnnualRevenueCurrency) //
                .fundamentalType(FundamentalType.ALPHA.name()) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());
        table.addAttribute(attr(InterfaceName.SpendAnalyticsSegment.name()) //
                .allowedDisplayNames(
                        Sets.newHashSet("SPENDANALYTICSSEGMENT", "SPEND_ANALYTICS_SEGMENT", "ACCOUNT_BUSINESS_SEGMENT")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.SpendAnalyticsSegment) //
                .fundamentalType(FundamentalType.ALPHA.name()) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());
        table.addAttribute(attr(InterfaceName.CustomerParentAccountID.name()) //
                .allowedDisplayNames(Sets.newHashSet("CUSTOMERPARENTACCOUNTID", "CUSTOMER_PARENT_ACCOUNT_ID")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.CustomerParentAccountID) //
                .fundamentalType(FundamentalType.ALPHA.name()) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());
        table.addAttribute(attr(InterfaceName.Longitude.name()) //
                .allowedDisplayNames(Sets.newHashSet("LONGITUDE")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.Longitude) //
                .fundamentalType(FundamentalType.ALPHA.name()) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());
        table.addAttribute(attr(InterfaceName.Latitude.name()) //
                .allowedDisplayNames(Sets.newHashSet("LATITUDE")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.Latitude) //
                .fundamentalType(FundamentalType.ALPHA.name()) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());
        Attribute eventAttr = attr("Event") //
                .allowedDisplayNames(Sets.newHashSet("EVENT", "WON", "P1_EVENT")) //
                .type(Schema.Type.BOOLEAN) //
                .interfaceName(InterfaceName.Event) //
                .logicalType(LogicalDataType.Event) //
                .fundamentalType(ModelingMetadata.FT_BOOLEAN) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build();
        if (!cdlSchema) {
            eventAttr.setNullable(false);
        }
        table.addAttribute(eventAttr);
        return table;
    }

    private Table getContactSchema(boolean cdlSchema, boolean enableEntityMatch) {
        Table table = createTable(SchemaInterpretation.Contact);
        if (!enableEntityMatch) {
            table.setPrimaryKey(createPrimaryKey(InterfaceName.ContactId.name()));
        }

        if (enableEntityMatch) {
            table.addAttribute(attr(InterfaceName.CustomerContactId.name()) //
                    .allowedDisplayNames(Sets.newHashSet("ID", "CONTACT", "EXTERNAL_ID", "CONTACT ID")) //
                    .type(Schema.Type.STRING) //
                    .interfaceName(InterfaceName.CustomerContactId) //
                    .logicalType(LogicalDataType.Id) //
                    .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                    .fundamentalType(ModelingMetadata.FT_ALPHA) //
                    .build());
        } else {
            table.addAttribute(attr(InterfaceName.ContactId.name()) //
                    .allowedDisplayNames(Sets.newHashSet("ID", "CONTACT", "EXTERNAL_ID", "CONTACT ID")) //
                    .type(Schema.Type.STRING) //
                    .notNull() //
                    .required() //
                    .interfaceName(InterfaceName.ContactId) //
                    .logicalType(LogicalDataType.Id) //
                    .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                    .fundamentalType(ModelingMetadata.FT_ALPHA) //
                    .build());
        }
        table.addAttribute(attr(InterfaceName.ContactName.name()) //
                .allowedDisplayNames(Sets.newHashSet("NAME", "CONTACT NAME", "CONTACT_NAME", "DISPLAY_NAME")) //
                .type(Schema.Type.STRING) //
                .defaultValueStr("").interfaceName(InterfaceName.ContactName) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .subcategory(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());
        table.addAttribute(attr("FirstName") //
                .allowedDisplayNames(Sets.newHashSet("FIRSTNAME", "FIRST NAME", "FIRST_NAME")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.FirstName) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .subcategory(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());
        table.addAttribute(attr("LastName") //
                .allowedDisplayNames(Sets.newHashSet("LASTNAME", "LAST NAME", "LAST_NAME")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.LastName) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .subcategory(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());
        if (enableEntityMatch) {
            table.addAttribute(attr(InterfaceName.CustomerAccountId.name()) //
                    .allowedDisplayNames(
                            Sets.newHashSet("ACCOUNT_ID", "ACCOUNTID", "ACCOUNT_EXTERNAL_ID", "ACCOUNT ID", "ACCOUNT")) //
                    .type(Schema.Type.STRING) //
                    .interfaceName(InterfaceName.CustomerAccountId) //
                    .logicalType(LogicalDataType.Id) //
                    .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                    .fundamentalType(ModelingMetadata.FT_ALPHA) //
                    .build());
        } else {
            table.addAttribute(attr(InterfaceName.AccountId.name()) //
                    .allowedDisplayNames(
                            Sets.newHashSet("ACCOUNT_ID", "ACCOUNTID", "ACCOUNT_EXTERNAL_ID", "ACCOUNT ID", "ACCOUNT")) //
                    .type(Schema.Type.STRING) //
                    .notNull() //
                    .required() //
                    .interfaceName(InterfaceName.AccountId) //
                    .logicalType(LogicalDataType.Id) //
                    .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                    .fundamentalType(ModelingMetadata.FT_ALPHA) //
                    .build());
        }
        table.addAttribute(attr(InterfaceName.Title.name()) //
                .allowedDisplayNames(Sets.newHashSet("TITLE")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.Title) //
                .approvedUsage(ModelingMetadata.MODEL_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
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
        table.addAttribute(attr(InterfaceName.DoNotMail.name()) //
                .allowedDisplayNames(Sets.newHashSet("DONOTMAIL", "DO_NOT_MAIL", "EMAIL_OPT_OUT")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.DoNotMail) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(FundamentalType.BOOLEAN.name()) //
                .build());
        table.addAttribute(attr(InterfaceName.DoNotCall.name()) //
                .allowedDisplayNames(Sets.newHashSet("DONOTCALL", "DO_NOT_CALL")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.DoNotCall) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(FundamentalType.BOOLEAN.name()) //
                .build());
        table.addAttribute(attr(InterfaceName.LeadStatus.name()) //
                .allowedDisplayNames(Sets.newHashSet("LEADSTATUS", "LEAD_STATUS")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.LeadStatus) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(FundamentalType.ALPHA.name()) //
                .build());
        table.addAttribute(attr(InterfaceName.LeadType.name()) //
                .allowedDisplayNames(Sets.newHashSet("LEADTYPE", "LEAD_TYPE")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.LeadType) //
                .fundamentalType(FundamentalType.ALPHA.name()) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());
        table.addAttribute(attr(InterfaceName.CreatedDate.name()) //
                .allowedDisplayNames(Sets.newHashSet("CREATEDDATE", "CREATED_DATE")) //
                .type(Schema.Type.LONG) //
                .interfaceName(InterfaceName.CreatedDate) //
                .logicalType(LogicalDataType.Date) //
                .fundamentalType(FundamentalType.DATE.name()) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE).build());
        table.addAttribute(attr(InterfaceName.LastModifiedDate.name()) //
                .allowedDisplayNames(Sets.newHashSet("LASTMODIFIEDDATE", "LAST_MODIFIED_DATE", "LASTMODIFIED")) //
                .type(Schema.Type.LONG) //
                .interfaceName(InterfaceName.LastModifiedDate) //
                .logicalType(LogicalDataType.Date) //
                .fundamentalType(FundamentalType.DATE.name()) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE).build());
        return table;
    }

    private Table getProductSchema() {
        Table table = createTable(SchemaInterpretation.Product);
        table.setPrimaryKey(createPrimaryKey(InterfaceName.ProductId.name()));

        table.addAttribute(attr(InterfaceName.ProductId.name()) //
                .allowedDisplayNames(Sets.newHashSet("ID", "PRODUCT_ID", "PRODUCT ID")) //
                .type(Schema.Type.STRING) //
                .required().notNull() //
                .interfaceName(InterfaceName.ProductId) //
                .logicalType(LogicalDataType.Id) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr(InterfaceName.ProductName.name()) //
                .allowedDisplayNames(Sets.newHashSet("NAME", "PRODUCT_NAME", "PRODUCT NAME")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.ProductName).approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .statisticalType(ModelingMetadata.NOMINAL_STAT_TYPE) //
                .failImportValidator().build());
        table.addAttribute(attr(InterfaceName.Description.name()) //
                .allowedDisplayNames(Sets.newHashSet("DESCRIPTION", "PRODUCT DESCRIPTION")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.Description).approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .statisticalType(ModelingMetadata.NOMINAL_STAT_TYPE) //
                .build());
        table.addAttribute(attr(InterfaceName.ProductBundle.name()) //
                .allowedDisplayNames(Sets.newHashSet("BUNDLE", "PRODUCT_BUNDLE", "PRODUCT BUNDLE")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.ProductBundle).approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .statisticalType(ModelingMetadata.NOMINAL_STAT_TYPE) //
                .build());
        table.addAttribute(attr(InterfaceName.ProductLine.name()) //
                .allowedDisplayNames(Sets.newHashSet("LINE", "PRODUCT_LINE", "PRODUCT LINE")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.ProductLine).approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .statisticalType(ModelingMetadata.NOMINAL_STAT_TYPE) //
                .build());
        table.addAttribute(attr(InterfaceName.ProductFamily.name()) //
                .allowedDisplayNames(Sets.newHashSet("FAMILY", "PRODUCT_FAMILY", "PRODUCT FAMILY")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.ProductFamily).approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .statisticalType(ModelingMetadata.NOMINAL_STAT_TYPE) //
                .build());
        table.addAttribute(attr(InterfaceName.ProductCategory.name()) //
                .allowedDisplayNames(Sets.newHashSet("CATEGORY", "PRODUCT_CATEGORY", "PRODUCT CATEGORY")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.ProductCategory).approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .statisticalType(ModelingMetadata.NOMINAL_STAT_TYPE) //
                .build());
        return table;
    }

    private Table getTransactionSchema() {
        Table table = createTable(SchemaInterpretation.Transaction);

        table.addAttribute(attr(InterfaceName.TransactionId.name()) //
                .allowedDisplayNames(Sets.newHashSet("ID", "TRANSACTION_ID", "TRANSACTION ID")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.TransactionId) //
                .logicalType(LogicalDataType.Id) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr(InterfaceName.AccountId.name()) //
                .allowedDisplayNames(
                        Sets.newHashSet("ACCOUNT_ID", "ACCOUNTID", "ACCOUNT_EXTERNAL_ID", "ACCOUNT ID", "ACCOUNT")) //
                .type(Schema.Type.STRING) //
                .notNull() //
                .required() //
                .interfaceName(InterfaceName.AccountId) //
                .logicalType(LogicalDataType.Id) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr(InterfaceName.ContactId.name()) //
                .allowedDisplayNames(
                        Sets.newHashSet("CONTACT_ID", "CONTACTID", "CONTACT_EXTERNAL_ID", "CONTACT ID", "CONTACT")) //
                .type(Schema.Type.STRING) //
                .defaultValueStr("").interfaceName(InterfaceName.ContactId) //
                .logicalType(LogicalDataType.Id) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr(InterfaceName.ProductId.name()) //
                .allowedDisplayNames(Sets.newHashSet("PRODUCT_ID", "PRODUCTID", "PRODUCT_EXTERNAL_ID", "PRODUCT ID")) //
                .type(Schema.Type.STRING) //
                .notNull() //
                .required() //
                .interfaceName(InterfaceName.ProductId) //
                .logicalType(LogicalDataType.Id) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr(InterfaceName.OrderId.name()) //
                .allowedDisplayNames(Sets.newHashSet("ORDER_ID", "ORDERID", "ORDER ID")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.OrderId) //
                .logicalType(LogicalDataType.Id) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr(InterfaceName.LastModifiedDate.name()) //
                .allowedDisplayNames(Sets.newHashSet("LASTMODIFIEDDATE", "LAST MODIFIED DATE", "LASTMODIFIED")) //
                .type(Schema.Type.LONG) //
                .interfaceName(InterfaceName.LastModifiedDate) //
                .logicalType(LogicalDataType.Timestamp) //
                .fundamentalType(FundamentalType.DATE.getName()) //
                .build());
        table.addAttribute(attr(InterfaceName.Quantity.name()) //
                .allowedDisplayNames(Sets.newHashSet("QUANTITY")) //
                .type(Schema.Type.DOUBLE) //
                .notNull() //
                .required() //
                .interfaceName(InterfaceName.Quantity) //
                .logicalType(LogicalDataType.Metric) //
                .fundamentalType(ModelingMetadata.FT_NUMERIC) //
                .build());
        table.addAttribute(attr(InterfaceName.Amount.name()) //
                .allowedDisplayNames(Sets.newHashSet("AMOUNT")) //
                .type(Schema.Type.DOUBLE) //
                .notNull() //
                .required() //
                .interfaceName(InterfaceName.Amount) //
                .logicalType(LogicalDataType.Metric) //
                .fundamentalType(ModelingMetadata.FT_NUMERIC) //
                .build());
        table.addAttribute(attr(InterfaceName.Cost.name()) //
                .allowedDisplayNames(Sets.newHashSet("COST")) //
                .type(Schema.Type.DOUBLE) //
                .interfaceName(InterfaceName.Cost) //
                .logicalType(LogicalDataType.Metric) //
                .fundamentalType(ModelingMetadata.FT_NUMERIC) //
                .build());
        table.addAttribute(attr(InterfaceName.TransactionTime.name()) //
                .allowedDisplayNames(Sets.newHashSet("TIMESTAMP", "TIME STAMP", "TRANSACTION_TIME", "TRANSACTION TIME",
                        "TRANSACTION_DATE", "TRANSACTION DATE")) //
                .type(Schema.Type.STRING) //
                .notNull() //
                .required() //
                .interfaceName(InterfaceName.TransactionTime) //
                .logicalType(LogicalDataType.Timestamp) //
                .fundamentalType(ModelingMetadata.FT_NUMERIC) //
                .build());
        table.addAttribute(attr(InterfaceName.TransactionType.name()) //
                .allowedDisplayNames(Sets.newHashSet("TYPE", "TRANSACTION_TYPE", "TRANSACTION TYPE")) //
                .type(Schema.Type.STRING) //
                .defaultValueStr("Purchase").interfaceName(InterfaceName.TransactionType) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr(InterfaceName.CustomTrxField.name()) //
                .allowedDisplayNames(Sets.newHashSet("CUSTOMFIELD", "CUSTOM_FIELD", "CUSTOM FIELD")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.CustomTrxField) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        return table;
    }

    private Table getRawTransactionSchema(boolean includeCdlTimestamps) {
        Table table = createTable(SchemaInterpretation.TransactionRaw);

        table.addAttribute(attr(InterfaceName.TransactionId.name()) //
                .allowedDisplayNames(Sets.newHashSet("ID", "TRANSACTION_ID", "TRANSACTION ID")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.TransactionId) //
                .logicalType(LogicalDataType.Id) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr(InterfaceName.AccountId.name()) //
                .allowedDisplayNames(
                        Sets.newHashSet("ACCOUNT_ID", "ACCOUNTID", "ACCOUNT_EXTERNAL_ID", "ACCOUNT ID", "ACCOUNT")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.AccountId) //
                .logicalType(LogicalDataType.Id) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr(InterfaceName.ContactId.name()) //
                .allowedDisplayNames(
                        Sets.newHashSet("CONTACT_ID", "CONTACTID", "CONTACT_EXTERNAL_ID", "CONTACT ID", "CONTACT")) //
                .type(Schema.Type.STRING) //
                .defaultValueStr("").interfaceName(InterfaceName.ContactId) //
                .logicalType(LogicalDataType.Id) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr(InterfaceName.ProductId.name()) //
                .allowedDisplayNames(Sets.newHashSet("PRODUCT_ID", "PRODUCTID", "PRODUCT_EXTERNAL_ID", "PRODUCT ID")) //
                .type(Schema.Type.STRING) //
                .notNull() //
                .required() //
                .interfaceName(InterfaceName.ProductId) //
                .logicalType(LogicalDataType.Id) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr(InterfaceName.OrderId.name()) //
                .allowedDisplayNames(Sets.newHashSet("ORDER_ID", "ORDERID", "ORDER ID")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.OrderId) //
                .logicalType(LogicalDataType.Id) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr(InterfaceName.LastModifiedDate.name()) //
                .allowedDisplayNames(Sets.newHashSet("LASTMODIFIEDDATE", "LAST MODIFIED DATE", "LASTMODIFIED")) //
                .type(Schema.Type.LONG) //
                .interfaceName(InterfaceName.LastModifiedDate) //
                .logicalType(LogicalDataType.Timestamp) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_YEAR) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());
        table.addAttribute(attr(InterfaceName.Quantity.name()) //
                .allowedDisplayNames(Sets.newHashSet("QUANTITY")) //
                .type(Schema.Type.LONG) //
                .interfaceName(InterfaceName.Quantity) //
                .logicalType(LogicalDataType.Metric) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_NUMERIC) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());
        table.addAttribute(attr(InterfaceName.Amount.name()) //
                .allowedDisplayNames(Sets.newHashSet("AMOUNT")) //
                .type(Schema.Type.LONG) //
                .interfaceName(InterfaceName.Amount) //
                .logicalType(LogicalDataType.Metric) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_NUMERIC) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());
        table.addAttribute(attr(InterfaceName.Cost.name()) //
                .allowedDisplayNames(Sets.newHashSet("COST")) //
                .type(Schema.Type.LONG) //
                .interfaceName(InterfaceName.Cost) //
                .logicalType(LogicalDataType.Metric) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_NUMERIC) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());
        table.addAttribute(attr(InterfaceName.TransactionTime.name()) //
                .allowedDisplayNames(Sets.newHashSet("TIMESTAMP", "TIME STAMP", "TRANSACTION_TIME", "TRANSACTION TIME")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.TransactionTime) //
                .logicalType(LogicalDataType.Timestamp) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_NUMERIC) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());
        table.addAttribute(attr(InterfaceName.TransactionType.name()) //
                .allowedDisplayNames(Sets.newHashSet("TYPE", "TRANSACTION_TYPE", "TRANSACTION TYPE")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.TransactionType) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());
        table.addAttribute(attr(InterfaceName.TransactionDate.name()) //
                .allowedDisplayNames(Sets.newHashSet("DATE", "TRANSACTION_DATE", "TRANSACTION DATE")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.TransactionDate) //
                .logicalType(LogicalDataType.Date) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr(InterfaceName.TransactionDayPeriod.name()) //
                .allowedDisplayNames(Sets.newHashSet("DAYPERIOD", "TRANSACTION_DAY_PERIOD", "TRANSACTION DAY PERIOD")) //
                .type(Schema.Type.INT) //
                .interfaceName(InterfaceName.TransactionDayPeriod) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr(InterfaceName.CustomTrxField.name()) //
                .allowedDisplayNames(Sets.newHashSet("CUSTOMFIELD", "CUSTOM_FIELD", "CUSTOM FIELD")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.CustomTrxField) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        if (includeCdlTimestamps) {
            table.addAttribute(attr(InterfaceName.CDLCreatedTime.name()) //
                    .allowedDisplayNames(Collections.emptySet()) //
                    .type(Schema.Type.LONG) //
                    .notNull() //
                    .interfaceName(InterfaceName.CDLCreatedTime) //
                    .logicalType(LogicalDataType.Timestamp) //
                    .fundamentalType(ModelingMetadata.FT_NUMERIC) //
                    .build());
            table.addAttribute(attr(InterfaceName.CDLUpdatedTime.name()) //
                    .allowedDisplayNames(Collections.emptySet()) //
                    .type(Schema.Type.LONG) //
                    .notNull() //
                    .interfaceName(InterfaceName.CDLUpdatedTime) //
                    .logicalType(LogicalDataType.Timestamp) //
                    .fundamentalType(ModelingMetadata.FT_NUMERIC) //
                    .build());
        }
        return table;
    }

    private Table getAggregatedTransactionSchema(SchemaInterpretation schema, boolean includeCdlTimestamps) {
        Table table = createTable(schema);
        table.addAttribute(attr(InterfaceName.AccountId.name()) //
                .allowedDisplayNames(
                        Sets.newHashSet("ACCOUNT_ID", "ACCOUNTID", "ACCOUNT_EXTERNAL_ID", "ACCOUNT ID", "ACCOUNT")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.AccountId) //
                .logicalType(LogicalDataType.Id) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr(InterfaceName.ContactId.name()) //
                .allowedDisplayNames(
                        Sets.newHashSet("CONTACT_ID", "CONTACTID", "CONTACT_EXTERNAL_ID", "CONTACT ID", "CONTACT")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.ContactId) //
                .logicalType(LogicalDataType.Id) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr(InterfaceName.ProductId.name()) //
                .allowedDisplayNames(Sets.newHashSet("ID", "PRODUCT_ID", "PRODUCT ID")) //
                .type(Schema.Type.STRING) //
                .notNull() //
                .interfaceName(InterfaceName.ProductId) //
                .logicalType(LogicalDataType.Id) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr(InterfaceName.TransactionType.name()) //
                .allowedDisplayNames(Sets.newHashSet("TYPE", "TRANSACTION_TYPE", "TRANSACTION TYPE")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.TransactionType) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr(InterfaceName.TransactionDate.name()) //
                .allowedDisplayNames(Sets.newHashSet("DATE", "TRANSACTION_DATE", "TRANSACTION DATE")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.TransactionDate) //
                .logicalType(LogicalDataType.Date) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr(InterfaceName.TransactionDayPeriod.name()) //
                .allowedDisplayNames(Sets.newHashSet("DAYPERIOD", "TRANSACTION_DAY_PERIOD", "TRANSACTION DAY PERIOD")) //
                .type(Schema.Type.INT) //
                .interfaceName(InterfaceName.TransactionDayPeriod) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr(InterfaceName.PeriodId.name()) //
                .allowedDisplayNames(Sets.newHashSet("DATE", "TRANSACTION_DATE", "TRANSACTION DATE")) //
                .type(Schema.Type.INT) //
                .interfaceName(InterfaceName.PeriodId) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr(InterfaceName.PeriodName.name()) //
                .allowedDisplayNames(Sets.newHashSet("PERIOD NAME")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.PeriodName) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr(InterfaceName.TotalAmount.name()) //
                .allowedDisplayNames(Sets.newHashSet("AMOUNT", "TOTAL AMOUNT")) //
                .type(Schema.Type.DOUBLE) //
                .notNull() //
                .interfaceName(InterfaceName.TotalAmount) //
                .logicalType(LogicalDataType.Metric) //
                .fundamentalType(ModelingMetadata.FT_NUMERIC) //
                .build());
        table.addAttribute(attr(InterfaceName.TotalCost.name()) //
                .allowedDisplayNames(Sets.newHashSet("COST", "TOTAL COST")) //
                .type(Schema.Type.DOUBLE) //
                .interfaceName(InterfaceName.TotalCost) //
                .logicalType(LogicalDataType.Metric) //
                .fundamentalType(ModelingMetadata.FT_NUMERIC) //
                .build());
        table.addAttribute(attr(InterfaceName.TotalQuantity.name()) //
                .allowedDisplayNames(Sets.newHashSet("QUANTITY", "TOTAL QUANTITY")) //
                .type(Schema.Type.LONG) //
                .notNull() //
                .interfaceName(InterfaceName.TotalQuantity) //
                .logicalType(LogicalDataType.Metric) //
                .fundamentalType(ModelingMetadata.FT_NUMERIC) //
                .build());
        table.addAttribute(attr(InterfaceName.TransactionCount.name()) //
                .allowedDisplayNames(Sets.newHashSet("TRANSACTION COUNT")) //
                .type(Schema.Type.LONG) //
                .notNull() //
                .interfaceName(InterfaceName.TransactionCount) //
                .logicalType(LogicalDataType.Metric) //
                .fundamentalType(ModelingMetadata.FT_NUMERIC) //
                .build());
        if (includeCdlTimestamps) {
            table.addAttribute(attr(InterfaceName.CDLCreatedTime.name()) //
                    .allowedDisplayNames(Collections.emptySet()) //
                    .type(Schema.Type.LONG) //
                    .notNull() //
                    .interfaceName(InterfaceName.CDLCreatedTime) //
                    .logicalType(LogicalDataType.Timestamp) //
                    .fundamentalType(FundamentalType.DATE.getName()) //
                    .build());
            table.addAttribute(attr(InterfaceName.CDLUpdatedTime.name()) //
                    .allowedDisplayNames(Collections.emptySet()) //
                    .type(Schema.Type.LONG) //
                    .notNull() //
                    .interfaceName(InterfaceName.CDLUpdatedTime) //
                    .logicalType(LogicalDataType.Timestamp) //
                    .fundamentalType(FundamentalType.DATE.getName()) //
                    .build());
        }
        return table;
    }

    private Table getDeleteAccountTemplateSchema() {
        Table table = createTable(SchemaInterpretation.DeleteAccountTemplate);

        table.addAttribute(attr(InterfaceName.AccountId.name()) //
                .allowedDisplayNames(Sets.newHashSet("ID", "ACCOUNT", "ACCOUNT_ID", "ACCOUNTID")) //
                .type(Schema.Type.STRING) //
                .notNull() //
                .required() //
                .interfaceName(InterfaceName.AccountId) //
                .logicalType(LogicalDataType.Id) //
                .fundamentalType(FundamentalType.ALPHA.name()) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());

        return table;
    }

    private Table getDeleteContactTemplateSchema() {
        Table table = createTable(SchemaInterpretation.DeleteContactTemplate);

        table.addAttribute(attr(InterfaceName.ContactId.name()) //
                .allowedDisplayNames(Sets.newHashSet("ID", "CONTACT", "CONTACT_ID", "CONTACTID")) //
                .type(Schema.Type.STRING) //
                .notNull() //
                .required() //
                .interfaceName(InterfaceName.ContactId) //
                .logicalType(LogicalDataType.Id) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());

        return table;
    }

    private Table getDeleteTransactionTemplateSchema() {
        Table table = createTable(SchemaInterpretation.DeleteTransactionTemplate);

        table.addAttribute(attr(InterfaceName.AccountId.name()) //
                .allowedDisplayNames(
                        Sets.newHashSet("ACCOUNT_ID", "ACCOUNTID", "ACCOUNT_EXTERNAL_ID", "ACCOUNT ID", "ACCOUNT")) //
                .type(Schema.Type.STRING) //
                .notNull() //
                .interfaceName(InterfaceName.AccountId) //
                .logicalType(LogicalDataType.Id) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr(InterfaceName.ContactId.name()) //
                .allowedDisplayNames(
                        Sets.newHashSet("CONTACT_ID", "CONTACTID", "CONTACT_EXTERNAL_ID", "CONTACT ID", "CONTACT")) //
                .type(Schema.Type.STRING) //
                .defaultValueStr("").interfaceName(InterfaceName.ContactId) //
                .logicalType(LogicalDataType.Id) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr(InterfaceName.ProductId.name()) //
                .allowedDisplayNames(Sets.newHashSet("PRODUCT_ID", "PRODUCTID", "PRODUCT_EXTERNAL_ID", "PRODUCT ID")) //
                .type(Schema.Type.STRING) //
                .notNull() //
                .interfaceName(InterfaceName.ProductId) //
                .logicalType(LogicalDataType.Id) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build());
        table.addAttribute(attr(InterfaceName.TransactionTime.name()) //
                .allowedDisplayNames(Sets.newHashSet("TIMESTAMP", "TIME STAMP", "TRANSACTION_TIME", "TRANSACTION TIME")) //
                .type(Schema.Type.STRING) //
                .notNull() //
                .required() //
                .interfaceName(InterfaceName.TransactionTime) //
                .logicalType(LogicalDataType.Timestamp) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_NUMERIC) //
                .category(ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION) //
                .build());

        return table;
    }

    private LastModifiedKey createLastModifiedKey(String columnName) {
        LastModifiedKey lmk = new LastModifiedKey();
        lmk.setDisplayName(columnName);
        lmk.setName(columnName);
        lmk.setLastModifiedTimestamp(DateTime.now().getMillis());
        lmk.setAttributes(Collections.singletonList(columnName));
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

    public List<Attribute> getMatchingAttributes(SchemaInterpretation schema) {
        Attribute website = attr("Website") //
                .allowedDisplayNames(Sets.newHashSet("WEBSITE")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.Website) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .statisticalType(ModelingMetadata.NOMINAL_STAT_TYPE) //
                .build();

        Attribute email = attr("Email") //
                .allowedDisplayNames(Sets.newHashSet("EMAIL", "EMAIL_ADDRESS")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.Email) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_EMAIL) //
                .statisticalType(ModelingMetadata.NOMINAL_STAT_TYPE) //
                .build();
        Attribute city = attr("City") //
                .allowedDisplayNames(Sets.newHashSet("CITY", "BILLING_CITY")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.City) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build();
        Attribute state = attr("State") //
                .allowedDisplayNames(Sets.newHashSet("STATE", "STATE PROVINCE", "STATE_PROVINCE", "BILLING_STATE",
                        "BILLING_PROVINCE")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.State) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build();
        Attribute country = attr("Country") //
                .allowedDisplayNames(Sets.newHashSet("COUNTRY", "BILLING_COUNTRY")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.Country) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build();
        Attribute postalCode = attr("PostalCode") //
                .allowedDisplayNames(Sets.newHashSet("ZIP", "POSTALCODE", "BILLING_ZIP", "POSTAL CODE", "POSTAL_CODE")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.PostalCode) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build();

        Attribute contactCompanyName = attr(InterfaceName.CompanyName.name()) //
                .allowedDisplayNames(Sets.newHashSet("COMPANY_NAME", "ACCOUNT_NAME")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.CompanyName) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build();

        Attribute accountCompanyName = attr(InterfaceName.CompanyName.name()) //
                .allowedDisplayNames(Sets.newHashSet("NAME", "COMPANY_NAME", "ACCOUNT_NAME", "DISPLAY_NAME")) //
                .type(Schema.Type.STRING) //
                .defaultValueStr("").interfaceName(InterfaceName.CompanyName) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build();

        Attribute phoneNumber = attr("PhoneNumber") //
                .allowedDisplayNames(Sets.newHashSet("PHONE", "PHONE_NUMBER")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.PhoneNumber) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build();

        Attribute duns = attr("DUNS") //
                .allowedDisplayNames(Sets.newHashSet("DUNS", "DUNS_NUMBER")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.DUNS) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .fundamentalType(ModelingMetadata.FT_ALPHA) //
                .build();

        Attribute address1 = attr(InterfaceName.Address_Street_1.name()) //
                .allowedDisplayNames(Sets.newHashSet("ADDRESS1", "ADDRESS_STREET_1", "ADDRESS_1")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.Address_Street_1) //
                .fundamentalType(FundamentalType.ALPHA.name()) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build();

        Attribute address2 = attr(InterfaceName.Address_Street_2.name()) //
                .allowedDisplayNames(Sets.newHashSet("ADDRESS2", "ADDRESS_STREET_2", "ADDRESS_2")) //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.Address_Street_2) //
                .fundamentalType(FundamentalType.ALPHA.name()) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build();

        List<Attribute> attrs = new ArrayList<>();
        if (schema == SchemaInterpretation.SalesforceAccount) {
            attrs.add(website);
            attrs.add(accountCompanyName);
            attrs.addAll(Arrays.asList(city, state, country, postalCode, phoneNumber, duns));
            attrs.forEach(a -> a.setCategory(Category.ACCOUNT_INFORMATION));
        } else if (schema == SchemaInterpretation.Account) {
            attrs.addAll(
                    Arrays.asList(website, accountCompanyName, duns, city, state, country, postalCode, phoneNumber));
            attrs.addAll(Arrays.asList(address1, address2));
            attrs.forEach(a -> a.setCategory(Category.ACCOUNT_ATTRIBUTES));
        } else if (schema == SchemaInterpretation.Contact || schema == SchemaInterpretation.SalesforceLead) {
            attrs.add(email);
            attrs.add(contactCompanyName);
            if (schema == SchemaInterpretation.Contact) {
                attrs.add(website);
                attrs.addAll(Arrays.asList(address1, address2));
            }
            attrs.addAll(Arrays.asList(city, state, country, postalCode, phoneNumber, duns));
            if (SchemaInterpretation.SalesforceLead.equals(schema)) {
                // needed for CSV downloads in LPI
                attrs.forEach(a -> a.setCategory(Category.LEAD_INFORMATION));
            } else {
                attrs.forEach(a -> a.setCategory(Category.CONTACT_ATTRIBUTES));
            }
        } else if (schema == SchemaInterpretation.ContactEntityMatch) {
            email.setDefaultValueStr("");
            attrs.addAll(Arrays.asList(email, website, accountCompanyName, duns, city, state, country, postalCode,
                    phoneNumber, address1, address2));
            attrs.forEach(a -> a.setCategory(Category.CONTACT_ATTRIBUTES));
        }
        return attrs;
    }

    public List<Attribute> matchingAttributes(BusinessEntity entity, boolean enableEntityMatch) {
        if (entity == BusinessEntity.Contact) {
            if (enableEntityMatch) {
                return getMatchingAttributes(SchemaInterpretation.ContactEntityMatch);
            } else {
                return getMatchingAttributes(SchemaInterpretation.Contact);
            }
        }
        if (entity == BusinessEntity.Account)
            return getMatchingAttributes(SchemaInterpretation.Account);
        return Collections.emptyList();
    }

    private static class AttributeBuilder {
        private Attribute attribute = new Attribute();

        AttributeBuilder() {
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

        public AttributeBuilder failImportValidator() {
            attribute.addValidator(new FailImportIfFieldIsEmpty());
            return this;
        }

        public AttributeBuilder notNull() {
            attribute.setNullable(false);
            return this;
        }

        public AttributeBuilder required() {
            attribute.setRequired(true);
            return this;
        }

        public AttributeBuilder defaultValueStr(String defaultValueStr) {
            attribute.setDefaultValueStr(defaultValueStr);
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
            List<String> list = new ArrayList<>(allowedDisplayNames);
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
