package com.latticeengines.pls.metadata.standardschemas;

import java.util.Arrays;

import org.apache.avro.Schema;
import org.joda.time.DateTime;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.LastModifiedKey;
import com.latticeengines.domain.exposed.metadata.PrimaryKey;
import com.latticeengines.domain.exposed.metadata.SemanticType;
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

        table.addAttribute(createAttribute("Id", Schema.Type.STRING, false, SemanticType.Id));
        table.addAttribute(createAttribute("Website", Schema.Type.STRING, false, SemanticType.Website));
        table.addAttribute(createAttribute("IsWon", Schema.Type.BOOLEAN, false, SemanticType.Event));

        table.addAttribute(createAttribute("Name", Schema.Type.STRING, SemanticType.CompanyName, "Website"));
        table.addAttribute(createAttribute("BillingCity", Schema.Type.STRING, SemanticType.City, "Website"));
        table.addAttribute(createAttribute("BillingState", Schema.Type.STRING, SemanticType.State, "Website"));
        table.addAttribute(createAttribute("BillingCountry", Schema.Type.STRING, SemanticType.Country, "Website"));
        table.addAttribute(createAttribute("BillingPostalCode", Schema.Type.STRING, SemanticType.PostalCode));

        table.addAttribute(createAttribute("Industry", Schema.Type.STRING, SemanticType.Industry));
        table.addAttribute(createAttribute("AnnualRevenue", Schema.Type.DOUBLE, SemanticType.AnnualRevenue));
        table.addAttribute(createAttribute("NumberOfEmployees", Schema.Type.INT, SemanticType.NumberOfEmployees));
        table.addAttribute(createAttribute("CreatedDate", Schema.Type.LONG, SemanticType.CreatedDate));
        table.addAttribute(createAttribute("LastModifiedDate", Schema.Type.LONG, SemanticType.LastModifiedDate));
        table.addAttribute(createAttribute("YearStarted", Schema.Type.STRING, SemanticType.YearStarted));
        table.addAttribute(createAttribute("Phone", Schema.Type.STRING, SemanticType.PhoneNumber));

        return table;
    }

    private Table getSalesforceLeadSchema() {
        Table table = createTable(SchemaInterpretation.SalesforceLead);
        table.setLastModifiedKey(createLastModifiedKey("LastModifiedDate"));
        table.setPrimaryKey(createPrimaryKey("Id"));

        table.addAttribute(createAttribute("Id", Schema.Type.STRING, false, SemanticType.Id));
        table.addAttribute(createAttribute("Email", Schema.Type.STRING, false, SemanticType.Email));
        table.addAttribute(createAttribute("IsConverted", Schema.Type.BOOLEAN, false, SemanticType.Event));

        table.addAttribute(createAttribute("Company", Schema.Type.STRING, SemanticType.CompanyName, "Email"));
        table.addAttribute(createAttribute("City", Schema.Type.STRING, SemanticType.City, "Email"));
        table.addAttribute(createAttribute("State", Schema.Type.STRING, SemanticType.State, "Email"));
        table.addAttribute(createAttribute("Country", Schema.Type.STRING, SemanticType.Country, "Email"));
        table.addAttribute(createAttribute("PostalCode", Schema.Type.STRING, SemanticType.PostalCode));

        table.addAttribute(createAttribute("CreatedDate", Schema.Type.LONG, SemanticType.CreatedDate));
        table.addAttribute(createAttribute("LastModifiedDate", Schema.Type.LONG, SemanticType.LastModifiedDate));
        table.addAttribute(createAttribute("FirstName", Schema.Type.STRING, SemanticType.FirstName));
        table.addAttribute(createAttribute("LastName", Schema.Type.STRING, SemanticType.LastName));
        table.addAttribute(createAttribute("Title", Schema.Type.STRING, SemanticType.Title));
        table.addAttribute(createAttribute("LeadSource", Schema.Type.STRING, SemanticType.LeadSource));
        table.addAttribute(createAttribute("Closed", Schema.Type.BOOLEAN, SemanticType.IsClosed));
        table.addAttribute(createAttribute("StageName", Schema.Type.STRING, SemanticType.StageName));
        table.addAttribute(createAttribute("Phone", Schema.Type.STRING, SemanticType.PhoneNumber));
        table.addAttribute(createAttribute("AnnualRevenue", Schema.Type.DOUBLE, SemanticType.AnnualRevenue));
        table.addAttribute(createAttribute("NumberOfEmployees", Schema.Type.INT, SemanticType.NumberOfEmployees));
        table.addAttribute(createAttribute("Industry", Schema.Type.STRING, SemanticType.Industry));
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

    private Attribute createAttribute(String name, Schema.Type dataType) {
        return createAttribute(name, dataType, null);
    }

    private Attribute createAttribute(String name, Schema.Type dataType, SemanticType semanticType) {
        return createAttribute(name, dataType, true, semanticType);
    }

    private Attribute createAttribute(String name, Schema.Type dataType, SemanticType semanticType,
            String otherFieldForValidator) {
        return createAttribute(name, dataType, true, semanticType, otherFieldForValidator);
    }

    private Attribute createAttribute(String name, Schema.Type dataType, boolean nullable, SemanticType semanticType) {
        return createAttribute(name, dataType, nullable, semanticType, null);
    }

    private Attribute createAttribute(String name, Schema.Type dataType, boolean nullable, SemanticType semanticType,
            String otherFieldForValidator) {
        Attribute attribute = new Attribute();
        attribute.setName(name);
        attribute.setPhysicalDataType(dataType.toString());
        attribute.setDisplayName(name);
        attribute.setNullable(nullable);
        attribute.setApprovedUsage(ModelingMetadata.MODEL_AND_ALL_INSIGHTS_APPROVED_USAGE);
        attribute.setSemanticType(semanticType);

        if (otherFieldForValidator != null) {
            attribute.addValidator(new RequiredIfOtherFieldIsEmpty(otherFieldForValidator));
        }

        return attribute;
    }

}
