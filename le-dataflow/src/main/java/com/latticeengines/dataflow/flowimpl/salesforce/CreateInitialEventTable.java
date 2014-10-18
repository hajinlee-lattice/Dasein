package com.latticeengines.dataflow.flowimpl.salesforce;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.CascadingDataFlowBuilder;

@Component("createInitialEventTable")
public class CreateInitialEventTable extends CascadingDataFlowBuilder {

    @Override
    public String constructFlowDefinition(Map<String, String> sources) {
        addSource("Lead", sources.get("Lead"));
        addSource("OpportunityContactRole", sources.get("OpportunityContactRole"));
        addSource("Contact", sources.get("Contact"));
        
        String f1Name = addFunction("Lead", //
                "Email.substring(Email.indexOf('@') + 1)", //
                new FieldList("Email"), //
                "DomainFromLead", String.class);
        String f2Name = addFunction("Contact", //
                "Email.substring(Email.indexOf('@') + 1)", //
                new FieldList("Email"), //
                "DomainFromContact", String.class);

        List<JoinCriteria> joinCriteria = new ArrayList<>();
        joinCriteria.add(new JoinCriteria(f1Name, new FieldList("DomainFromLead"), null));
        joinCriteria.add(new JoinCriteria(f2Name, new FieldList("DomainFromContact"), null));
        String leadAndContactOperatorName = addInnerJoin(joinCriteria);
        
        joinCriteria = new ArrayList<>();
        joinCriteria.add(new JoinCriteria("OpportunityContactRole", new FieldList("ContactId"), null));
        joinCriteria.add(new JoinCriteria(leadAndContactOperatorName, new FieldList("Id_1"), null));
        
        String contactRoleJoinedWithLeadAndContactOperatorName = addInnerJoin(joinCriteria);
        
        
        
        
        List<GroupByCriteria> groupByCriteria = new ArrayList<>();
        groupByCriteria.add(new GroupByCriteria("AnnualRevenue", "MaxRevenue", GroupByCriteria.AggregationType.MAX));
        groupByCriteria.add(new GroupByCriteria("NumberOfEmployees", "TotalEmployees", GroupByCriteria.AggregationType.SUM));
        String lastAggregatedOperatorName = addGroupBy(contactRoleJoinedWithLeadAndContactOperatorName, new FieldList("DomainFromLead", "IsPrimary"), groupByCriteria);

        // SELECT DomainFromLead, IsPrimary, MaxRevenue, TotalEmployees FROM T WHERE MaxRevenue > 0
        String lastFilter = addFilter(lastAggregatedOperatorName, "MaxRevenue > 0 && TotalEmployees > 0", new FieldList("MaxRevenue", "TotalEmployees"));
        
        
        return lastFilter;
    }

}
