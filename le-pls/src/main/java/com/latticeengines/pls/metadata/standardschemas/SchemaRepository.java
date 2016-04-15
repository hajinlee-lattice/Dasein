package com.latticeengines.pls.metadata.standardschemas;

import java.util.Arrays;

import org.apache.avro.Schema;
import org.joda.time.DateTime;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LastModifiedKey;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.PrimaryKey;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.validators.RequiredIfOtherFieldIsEmpty;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;

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

    public Table getSchema(SchemaInterpretation schema) {
        switch (schema) {
        case SalesforceAccount:
            return getSalesforceAccountSchema();
        case SalesforceLead:
            return getSalesforceLeadSchema();
        default:
            throw new RuntimeException(String.format("Unsupported schema %s", schema));
        }
    }

    private Table getSalesforceAccountSchema() {
        Table table = createTable(SchemaInterpretation.SalesforceAccount);
        table.setLastModifiedKey(createLastModifiedKey("LastModifiedDate"));
        table.setPrimaryKey(createPrimaryKey("Id"));

        table.addAttribute(attr("Id") //
                .type(Schema.Type.STRING) //
                .required() //
                .interfaceName(InterfaceName.Id) //
                .logicalType(LogicalDataType.Id) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());
        table.addAttribute(attr("Website") //
                .type(Schema.Type.STRING) //
                .required().interfaceName(InterfaceName.Website) //
                .build());
        table.addAttribute(attr("Event") //
                .type(Schema.Type.BOOLEAN) //
                .required() //
                .interfaceName(InterfaceName.Event) //
                .logicalType(LogicalDataType.Event) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());

        table.addAttribute(attr("CompanyName") //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.CompanyName) //
                .withValidator("Website") //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());
        table.addAttribute(attr("City") //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.City) //
                .withValidator("Website") //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());
        table.addAttribute(attr("State") //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.State) //
                .withValidator("Website") //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());
        table.addAttribute(attr("Country") //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.Country) //
                .withValidator("Website") //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());
        table.addAttribute(attr("PostalCode") //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.PostalCode) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());

        table.addAttribute(attr("Industry") //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.Industry) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());
        table.addAttribute(attr("AnnualRevenue") //
                .type(Schema.Type.DOUBLE) //
                .interfaceName(InterfaceName.AnnualRevenue) //
                .build());
        table.addAttribute(attr("NumberOfEmployees") //
                .type(Schema.Type.INT) //
                .interfaceName(InterfaceName.NumberOfEmployees) //
                .build());
        table.addAttribute(attr("CreatedDate") //
                .type(Schema.Type.LONG) //
                .interfaceName(InterfaceName.CreatedDate) //
                .logicalType(LogicalDataType.Date) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());
        table.addAttribute(attr("LastModifiedDate") //
                .type(Schema.Type.LONG) //
                .interfaceName(InterfaceName.LastModifiedDate) //
                .logicalType(LogicalDataType.Date) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());
        table.addAttribute(attr("YearStarted") //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.YearStarted) //
                .approvedUsage(ModelingMetadata.MODEL_APPROVED_USAGE) //
                .build());
        table.addAttribute(attr("PhoneNumber") //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.PhoneNumber) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());

        return table;
    }

    private Table getSalesforceLeadSchema() {
        Table table = createTable(SchemaInterpretation.SalesforceLead);
        table.setLastModifiedKey(createLastModifiedKey("LastModifiedDate"));
        table.setPrimaryKey(createPrimaryKey("Id"));

        table.addAttribute(attr("Id") //
                .type(Schema.Type.STRING) //
                .required() //
                .interfaceName(InterfaceName.Id) //
                .logicalType(LogicalDataType.Id) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());
        table.addAttribute(attr("Email") //
                .type(Schema.Type.STRING) //
                .required() //
                .interfaceName(InterfaceName.Email) //
                .build());
        table.addAttribute(attr("Event") //
                .type(Schema.Type.BOOLEAN) //
                .required().interfaceName(InterfaceName.Event) //
                .logicalType(LogicalDataType.Event) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());

        table.addAttribute(attr("CompanyName") //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.CompanyName) //
                .withValidator("Email") //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());
        table.addAttribute(attr("City") //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.City) //
                .withValidator("Email") //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());
        table.addAttribute(attr("State") //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.State) //
                .withValidator("Email") //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());
        table.addAttribute(attr("Country") //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.Country) //
                .withValidator("Email") //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());
        table.addAttribute(attr("PostalCode") //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.PostalCode) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());

        table.addAttribute(attr("CreatedDate") //
                .type(Schema.Type.LONG) //
                .interfaceName(InterfaceName.CreatedDate) //
                .logicalType(LogicalDataType.Date) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());
        table.addAttribute(attr("LastModifiedDate") //
                .type(Schema.Type.LONG) //
                .interfaceName(InterfaceName.LastModifiedDate) //
                .logicalType(LogicalDataType.Date) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());
        table.addAttribute(attr("FirstName") //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.FirstName) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());
        table.addAttribute(attr("LastName") //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.LastName) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());
        table.addAttribute(attr("Title") //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.Title) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());
        table.addAttribute(attr("LeadSource") //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.LeadSource) //
                .build());
        table.addAttribute(attr("IsClosed") //
                .type(Schema.Type.BOOLEAN) //
                .interfaceName(InterfaceName.IsClosed) //
                .logicalType(LogicalDataType.Opportunity) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());
        table.addAttribute(attr("StageName") //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.StageName) //
                .logicalType(LogicalDataType.Opportunity) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());
        table.addAttribute(attr("PhoneNumber") //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.PhoneNumber) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
                .build());
        table.addAttribute(attr("AnnualRevenue") //
                .type(Schema.Type.DOUBLE) //
                .interfaceName(InterfaceName.AnnualRevenue) //
                .build());
        table.addAttribute(attr("NumberOfEmployees") //
                .type(Schema.Type.INT) //
                .interfaceName(InterfaceName.NumberOfEmployees) //
                .build());
        table.addAttribute(attr("Industry") //
                .type(Schema.Type.STRING) //
                .interfaceName(InterfaceName.Industry) //
                .approvedUsage(ModelingMetadata.NONE_APPROVED_USAGE) //
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

    private PrimaryKey createPrimaryKey(String columnName) {
        PrimaryKey pk = new PrimaryKey();
        pk.setDisplayName(columnName);
        pk.setName(columnName);
        pk.setAttributes(Arrays.asList(columnName));
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
        return builder;
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
    }

}
