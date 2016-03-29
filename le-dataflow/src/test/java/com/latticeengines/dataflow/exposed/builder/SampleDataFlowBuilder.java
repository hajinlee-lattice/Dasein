package com.latticeengines.dataflow.exposed.builder;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.common.Aggregation;
import com.latticeengines.dataflow.exposed.builder.common.AggregationType;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.FieldMetadata;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

@Component("sampleDataFlowBuilder")
public class SampleDataFlowBuilder extends TypesafeDataFlowBuilder<DataFlowParameters> {

    /**
     * SELECT Domain, MaxRevenue, TotalEmployees FROM ( SELECT Domain,
     * MAX(AnnualRevenue) MaxRevenue, SUM(NumberOfEmployees) TotalEmployees FROM
     * ( SELECT a.*, b.*, a.Email.substring(a.Email.indexOf('@') + 1) Domain
     * FROM Lead a, Opportunity b WHERE a.ConvertedOpportunityId = b.Id ) GROUP
     * BY Domain ) WHERE MaxRevenue > 0 AND TotalEmployees > 0
     */

    @Override
    public Node construct(DataFlowParameters parameters) {
        Node lead = addSource("Lead");
        Node oppty = addSource("Opportunity");

        // SELECT a.*, b.* FROM lead a, oppty b WHERE a.ConvertedOpportunityId =
        // b.Id
        Node last = lead.innerJoin(new FieldList("ConvertedOpportunityId"), oppty, new FieldList("Id"));

        last = last.addFunction("Email == null ? \"\" : Email.substring(Email.indexOf('@') + 1)", //
                new FieldList("Email"), //
                new FieldMetadata("Domain", String.class));

        // SELECT Domain, MAX(AnnualRevenue) MaxRevenue, SUM(NumberOfEmployees)
        // TotalEmployees
        // FROM T GROUP BY Domain
        List<Aggregation> aggregation = new ArrayList<>();
        aggregation.add(new Aggregation("AnnualRevenue", "MaxRevenue", AggregationType.MAX));
        aggregation.add(new Aggregation("NumberOfEmployees", "TotalEmployees", AggregationType.SUM));
        last = last.groupBy(new FieldList("Domain"), aggregation);

        last = last.checkpoint("checkpoint");

        // SELECT Domain, MAX(AnnualRevenue) MaxRevenue, SUM(NumberOfEmployees)
        // TotalEmployees, HashCode(Domain) DomainHashCode
        // FROM T GROUP BY Domain
        last = last.addJythonFunction( //
                "com.latticeengines.dataflow.exposed.builder", //
                "encoder", //
                "encode", //
                new FieldList("Domain"), //
                new FieldMetadata("DomainHashCode", Long.class));

        return last;
    }

}
