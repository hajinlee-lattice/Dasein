package com.latticeengines.dataflow.flowimpl.salesforce;

import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.CascadingDataFlowBuilder;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

@Component("createInitialEventTable")
public class CreateInitialEventTable extends CascadingDataFlowBuilder {

    @Override
    public String constructFlowDefinition(DataFlowContext dataFlowCtx, Map<String, String> sources) {
        setDataFlowCtx(dataFlowCtx);
        addSource("Lead", sources.get("Lead"));
        addSource("Contact", sources.get("Contact"));
        addSource("OpportunityContactRole", sources.get("OpportunityContactRole"));

        String lead$contact = addJoin("Lead", //
                new FieldList("Email"), //
                "Contact", //
                new FieldList("Email"), //
                JoinType.OUTER);

        String removeNullsForEmailsOnBothSides = addFilter(
                lead$contact, //
                "(Email == null || Email.trim().isEmpty()) && (Contact__Email == null || Contact__Email.trim().isEmpty())", //
                new FieldList("Email", "Contact__Email"));

        String normalizeEmail = addFunction(removeNullsForEmailsOnBothSides, //
                "Email != null ? Email : Contact__Email", //
                new FieldList("Email", "Contact__Email"), //
                new FieldMetadata("CleanEmail", String.class));

        FieldMetadata domain = new FieldMetadata("Domain", String.class);
        domain.setPropertyValue("length", "255");
        domain.setPropertyValue("precision", "0");
        domain.setPropertyValue("scale", "0");
        domain.setPropertyValue("logicalType", "domain");

        String addDomain = addFunction(normalizeEmail, //
                "CleanEmail.substring(CleanEmail.indexOf('@') + 1)", //
                new FieldList("CleanEmail"), //
                domain);

        String opptyContactRole$lead$contact = addInnerJoin(addDomain, //
                new FieldList("Contact__Id"), //
                "OpportunityContactRole", //
                new FieldList("ContactId"));

        String propDataHash = addMD5(opptyContactRole$lead$contact, //
                new FieldList("Domain", "Company", "City", "State", "Country"), //
                "PropDataHash");

        return propDataHash;
    }

    @Override
    public Node constructFlowDefinition(DataFlowParameters parameters) {
        throw new IllegalStateException("Not supported");
    }
}
