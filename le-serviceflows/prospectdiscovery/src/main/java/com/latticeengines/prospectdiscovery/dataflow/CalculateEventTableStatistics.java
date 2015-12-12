package com.latticeengines.prospectdiscovery.dataflow;

import java.util.ArrayList;
import java.util.List;

import com.latticeengines.domain.exposed.metadata.Table;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

@Component("calculateEventTableStatistics")
public class CalculateEventTableStatistics extends TypesafeDataFlowBuilder<DataFlowParameters> {

    @Override
    public Node construct(DataFlowParameters parameters) {
        Node eventTable = addSource("EventTable");
        Node account = addSource("Account");
        Node opportunity = addSource("Opportunity");
        Node contact = addSource("Contact");
        Node lead = addSource("Lead");

        // Account stats
        Node output = addTotal("TotalAccounts", account);
        output = addTotal("TotalAccountsWithContacts", eventTable, //
                "HasContacts", new FieldList("HasContacts"), output);
        output = addTotal("TotalAccountsWithOpportunities", eventTable, //
                "HasOpportunities", new FieldList("HasOpportunities"), output);

        output = addTotal("TotalUniqueAccounts", eventTable, output);

        output = addDateRanges("AccountDateRangeBegin", "AccountDateRangeEnd", account, getSourceMetadata("Account"), output);

        // Contact stats
        output = addTotal("TotalContacts", contact, output);
        output = addDateRanges("ContactDateRangeBegin", "ContactDateRangeEnd", contact, getSourceMetadata("Contact"), output);

        // Lead stats
        output = addTotal("TotalLeads", lead, output);
        output = addDateRanges("LeadDateRangeBegin", "LeadDateRangeEnd", contact, getSourceMetadata("Lead"), output);

        // Opportunity stats
        output = addTotal("TotalOpportunities", opportunity, output);
        output = addTotal("TotalClosedOpportunities", opportunity, "StageName.equals(\"Closed\")", new FieldList(
                "StageName"), output);
        output = addTotal("TotalClosedWonOpportunities", opportunity, "StageName.equals(\"Closed Won\")",
                new FieldList("StageName"), output);
        output = addDateRanges("OpportunityDateRangeBegin", "OpportunityDateRangeEnd", opportunity, getSourceMetadata("Opportunity"), output);
        return output;
    }

    private Node addTotal(String fieldName, Node source) {
        return addTotal(fieldName, source, null);
    }

    private Node addTotal(String fieldName, Node source, Node output) {
        return addTotal(fieldName, source, null, null, output);
    }

    private Node addTotal(String fieldName, Node source, String constraint, FieldList constraintFields, Node output) {
        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation("Id", fieldName, Aggregation.AggregationType.COUNT));

        if (constraint != null && constraintFields != null) {
            source = source.filter(constraint, constraintFields);
        }
        Node aggregated = source.groupBy(new FieldList(), aggregations);
        if (output != null) {
            return output.combine(aggregated);
        }
        return aggregated;
    }

    private Node addDateRanges(String minDateColumn, String maxDateColumn, Node source, Table sourceMetadata, Node output) {
        String timestampField = sourceMetadata.getLastModifiedKey().getName();
        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation(timestampField, minDateColumn, Aggregation.AggregationType.MIN));
        output = addAggregation(aggregations, source, output);

        aggregations.clear();
        aggregations.add(new Aggregation(timestampField, maxDateColumn, Aggregation.AggregationType.MAX));
        output = addAggregation(aggregations, source, output);
        return output;
    }

    private Node addAggregation(List<Aggregation> aggregations, Node source, Node output) {
        source = source.groupBy(new FieldList(), aggregations);

        if (output != null) {
            return output.combine(source);
        }
        return source;
    }
}
