package com.latticeengines.dataflow.exposed.builder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

@Component("sampleDataFlowBuilder")
public class SampleDataFlowBuilder extends CascadingDataFlowBuilder {
    
    public SampleDataFlowBuilder() {
        super(true);
    }

    /**
     * SELECT Company, MaxRevenue, TotalEmployees FROM 
     * (
     *     SELECT Company, MAX(AnnualRevenue) MaxRevenue, SUM(NumberOfEmployees) TotalEmployees FROM 
     *     (
     *         SELECT a.*, b.* FROM lead a, oppty b WHERE a.ConvertedOpportunityId = b.Id
     *     ) GROUP BY Company
     * ) WHERE MaxRevenue > 0 AND TotalEmployees > 0
     */
    @Override
    public String constructFlowDefinition(Map<String, String> sources) {
        
        // SELECT a.*, b.* FROM lead a, oppty b WHERE a.ConvertedOpportunityId = b.Id
        List<JoinCriteria> joinCriteria = new ArrayList<>();
        for (Map.Entry<String, String> entry : sources.entrySet()) {
            String name = entry.getKey();
            addSource(name, entry.getValue());
            
            switch (name) {
            
            case "lead":
                FieldList joinFieldsForLead = new FieldList("ConvertedOpportunityId");
                joinCriteria.add(new JoinCriteria(name, joinFieldsForLead, null));
                break;

            case "oppty":
                FieldList joinFieldsForOppty = new FieldList("Id");
                joinCriteria.add(new JoinCriteria(name, joinFieldsForOppty, null));
                break;
            }
            
        }
        
        String joinOperatorName = addInnerJoin(joinCriteria);
        
        // SELECT Company, MAX(AnnualRevenue) MaxRevenue, SUM(NumberOfEmployees) TotalEmployees FROM T GROUP BY Company
        List<GroupByCriteria> groupByCriteria = new ArrayList<>();
        groupByCriteria.add(new GroupByCriteria("AnnualRevenue", "MaxRevenue", GroupByCriteria.AggregationType.MAX));
        groupByCriteria.add(new GroupByCriteria("NumberOfEmployees", "TotalEmployees", GroupByCriteria.AggregationType.SUM));
        String lastAggregatedOperatorName = addGroupBy(joinOperatorName, new FieldList("Company"), groupByCriteria);

        // SELECT Company, MaxRevenue, TotalEmployees FROM T WHERE MaxRevenue > 0
        String lastFilter = addFilter(lastAggregatedOperatorName, "MaxRevenue > 0 && TotalEmployees > 0", new FieldList("MaxRevenue", "TotalEmployees"));
       
        return lastFilter;
    }

}
