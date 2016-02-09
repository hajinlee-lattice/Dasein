package com.latticeengines.metadata.exposed.standardschemas;

import java.util.Arrays;

import org.apache.avro.Schema;
import org.joda.time.DateTime;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.LastModifiedKey;
import com.latticeengines.domain.exposed.metadata.SchemaInterpretation;
import com.latticeengines.domain.exposed.metadata.Table;

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

        table.addAttribute(createAttribute("Id", Schema.Type.STRING, false));
        table.addAttribute(createAttribute("Website", Schema.Type.STRING, false));
        table.addAttribute(createAttribute("Won", Schema.Type.BOOLEAN, false));

        table.addAttribute(createAttribute("Name", Schema.Type.STRING));
        table.addAttribute(createAttribute("BillingCity", Schema.Type.STRING));
        table.addAttribute(createAttribute("BillingState", Schema.Type.STRING));
        table.addAttribute(createAttribute("BillingPostalCode", Schema.Type.STRING));
        table.addAttribute(createAttribute("BillingCountry", Schema.Type.STRING));
        table.addAttribute(createAttribute("Industry", Schema.Type.STRING));
        table.addAttribute(createAttribute("AnnualRevenue", Schema.Type.DOUBLE));
        table.addAttribute(createAttribute("NumberOfEmployees", Schema.Type.INT));
        table.addAttribute(createAttribute("CreatedDate", Schema.Type.LONG));
        table.addAttribute(createAttribute("LastModifiedDate", Schema.Type.LONG));
        table.addAttribute(createAttribute("YearStarted", Schema.Type.STRING));
        table.addAttribute(createAttribute("Phone", Schema.Type.STRING));

        return table;
    }

    private Table getSalesforceLeadSchema() {
        Table table = createTable(SchemaInterpretation.SalesforceLead);
        table.setLastModifiedKey(createLastModifiedKey("LastModifiedDate"));

        table.addAttribute(createAttribute("Id", Schema.Type.STRING, false));
        table.addAttribute(createAttribute("Email", Schema.Type.STRING, false));
        table.addAttribute(createAttribute("IsWon", Schema.Type.BOOLEAN, false));
        table.addAttribute(createAttribute("Company", Schema.Type.STRING));
        table.addAttribute(createAttribute("City", Schema.Type.STRING));
        table.addAttribute(createAttribute("State", Schema.Type.STRING));
        table.addAttribute(createAttribute("Country", Schema.Type.STRING));
        table.addAttribute(createAttribute("CreatedDate", Schema.Type.LONG));
        table.addAttribute(createAttribute("LastModifiedDate", Schema.Type.LONG));
        table.addAttribute(createAttribute("PostalCode", Schema.Type.STRING));
        table.addAttribute(createAttribute("FirstName", Schema.Type.STRING));
        table.addAttribute(createAttribute("LastName", Schema.Type.STRING));
        table.addAttribute(createAttribute("Title", Schema.Type.STRING));
        table.addAttribute(createAttribute("LeadSource", Schema.Type.STRING));
        table.addAttribute(createAttribute("Closed", Schema.Type.BOOLEAN));
        table.addAttribute(createAttribute("StageName", Schema.Type.STRING));
        table.addAttribute(createAttribute("Phone", Schema.Type.STRING));
        table.addAttribute(createAttribute("AnnualRevenue", Schema.Type.DOUBLE));
        table.addAttribute(createAttribute("NumberOfEmployees", Schema.Type.INT));
        table.addAttribute(createAttribute("Industry", Schema.Type.STRING));
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

    private Table createTable(SchemaInterpretation interpretation) {
        Table table = new Table();
        String interpretationString = interpretation.toString();
        table.setInterpretation(interpretationString);
        table.setName(interpretationString);
        table.setDisplayName(interpretationString);
        return table;
    }

    private Attribute createAttribute(String name, Schema.Type dataType) {
        return createAttribute(name, dataType, true);
    }

    private Attribute createAttribute(String name, Schema.Type dataType, boolean nullable) {
        Attribute attribute = new Attribute();
        attribute.setName(name);
        attribute.setPhysicalDataType(dataType.toString());
        attribute.setDisplayName(name);
        attribute.setNullable(nullable);
        return attribute;
    }

}
