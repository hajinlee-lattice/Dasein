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

        String f1Name = addFunction("Lead", //
                "Email.substring(Email.indexOf('@') + 1)", //
                new FieldList("Email"), //
                "DomainForLead", String.class);
        String f2Name = addFunction("Contact", //
                "Email.substring(Email.indexOf('@') + 1)", //
                new FieldList("Email"), //
                "DomainForContact", String.class);

        String lead$contact = addInnerJoin(f1Name, //
                new FieldList("DomainForLead"), //
                f2Name, //
                new FieldList("DomainForContact"));

        String opptyContactRole$lead$contact = addInnerJoin("OpportunityContactRole", //
                new FieldList("ContactId"), //
                lead$contact, //
                new FieldList("Contact$Id"));

        String opptyContactRole$lead$contact$modified = addFunction(opptyContactRole$lead$contact, //
                "Contact$Id + 1", //
                new FieldList("Contact$Id"), //
                "Contact$Id", Integer.class);
        
        String withRowid = addRowId(opptyContactRole$lead$contact$modified, //
                "RowId", opptyContactRole$lead$contact$modified);

        return withRowid;
    }
}
