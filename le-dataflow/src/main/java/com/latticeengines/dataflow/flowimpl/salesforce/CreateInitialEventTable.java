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

        FieldMetadata domain = new FieldMetadata("Domain", String.class);
        domain.setPropertyValue("length", "255");
        domain.setPropertyValue("precision", "0");
        domain.setPropertyValue("scale", "0");
        domain.setPropertyValue("logicalType", "domain");
        String f1Name = addFunction("Lead", //
                "Email.substring(Email.indexOf('@') + 1)", //
                new FieldList("Email"), //
                domain);

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
                new FieldList("Domain"), //
                f2Name, //
                new FieldList("DomainForContact"));

        String opptyContactRole$lead$contact = addInnerJoin(lead$contact, //
                new FieldList("Contact__Id"), //
                "OpportunityContactRole", //
                new FieldList("ContactId"));

        String propDataHash = addMD5(opptyContactRole$lead$contact, //
                new FieldList("Domain", "Company", "City", "Country"), //
                "PropDataHash");

        return propDataHash;
    }
}
