package com.latticeengines.dataflow.flowimpl.salesforce;

import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.CascadingDataFlowBuilder;

@Component("createInitialEventTable")
public class CreateInitialEventTable extends CascadingDataFlowBuilder {

    @Override
    public String constructFlowDefinition(Map<String, String> sources) {
        addSource("Lead", sources.get("Lead"));
        addSource("Contact", sources.get("Contact"));
        addSource("OpportunityContactRole", sources.get("OpportunityContactRole"));

        FieldMetadata domainForLead = new FieldMetadata("DomainForLead", String.class);
        domainForLead.setPropertyValue("length", "255");
        domainForLead.setPropertyValue("precision", "0");
        domainForLead.setPropertyValue("scale", "0");
        domainForLead.setPropertyValue("logicalType", "domain");
        String f1Name = addFunction("Lead", //
                "Email.substring(Email.indexOf('@') + 1)", //
                new FieldList("Email"), //
                domainForLead);
        
        FieldMetadata domainForContact = new FieldMetadata("DomainForContact", String.class);
        domainForContact.setPropertyValue("length", "255");
        domainForContact.setPropertyValue("precision", "0");
        domainForContact.setPropertyValue("scale", "0");
        domainForContact.setPropertyValue("logicalType", "domain");
        String f2Name = addFunction("Contact", //
                "Email.substring(Email.indexOf('@') + 1)", //
                new FieldList("Email"), //
                domainForContact);

        String lead$contact = addInnerJoin(f1Name, //
                new FieldList("DomainForLead"), //
                f2Name, //
                new FieldList("DomainForContact"));

        String opptyContactRole$lead$contact = addInnerJoin(lead$contact, //
                new FieldList("Contact__Id"), //
                "OpportunityContactRole", //
                new FieldList("ContactId"));
        
        String withRowid = addRowId(opptyContactRole$lead$contact, //
                "RowId", opptyContactRole$lead$contact);

        return withRowid;
    }
}
