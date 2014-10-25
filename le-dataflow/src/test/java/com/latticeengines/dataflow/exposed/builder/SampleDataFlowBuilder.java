package com.latticeengines.dataflow.exposed.builder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

@Component("sampleDataFlowBuilder")
public class SampleDataFlowBuilder extends CascadingDataFlowBuilder {
    
    public SampleDataFlowBuilder() {
        super(true);
    }

    /**
     * SELECT Domain, MaxRevenue, TotalEmployees FROM 
     * (
     *     SELECT Domain, MAX(AnnualRevenue) MaxRevenue, SUM(NumberOfEmployees) TotalEmployees FROM 
     *     (
     *         SELECT a.*, b.*, a.Email.substring(a.Email.indexOf('@') + 1) Domain 
     *         FROM Lead a, Opportunity b WHERE a.ConvertedOpportunityId = b.Id
     *     ) GROUP BY Domain
     * ) WHERE MaxRevenue > 0 AND TotalEmployees > 0
     */
    @Override
    public String constructFlowDefinition(DataFlowContext dataFlowCtx, Map<String, String> sources) {
        
        // SELECT a.*, b.* FROM lead a, oppty b WHERE a.ConvertedOpportunityId = b.Id
        String joinOperatorName = addInnerJoin("Lead", new FieldList("ConvertedOpportunityId"), "Opportunity", new FieldList("Id"));
        
        String functionOperatorName = addFunction(joinOperatorName, //
                "Email.substring(Email.indexOf('@') + 1)", //
                new FieldList("Email"), //
                new FieldMetadata("Domain", String.class));
        
        // SELECT Domain, MAX(AnnualRevenue) MaxRevenue, SUM(NumberOfEmployees) TotalEmployees FROM T GROUP BY Domain
        List<GroupByCriteria> groupByCriteria = new ArrayList<>();
        groupByCriteria.add(new GroupByCriteria("AnnualRevenue", "MaxRevenue", GroupByCriteria.AggregationType.MAX));
        groupByCriteria.add(new GroupByCriteria("NumberOfEmployees", "TotalEmployees", GroupByCriteria.AggregationType.SUM));
        String lastAggregatedOperatorName = addGroupBy(functionOperatorName, new FieldList("Domain"), groupByCriteria);

        // SELECT Domain, MaxRevenue, TotalEmployees FROM T WHERE MaxRevenue > 0
        String lastFilter = addFilter(lastAggregatedOperatorName, "MaxRevenue > 0 && TotalEmployees > 0", new FieldList("MaxRevenue", "TotalEmployees"));
       
        return lastFilter;
    }
    

}
