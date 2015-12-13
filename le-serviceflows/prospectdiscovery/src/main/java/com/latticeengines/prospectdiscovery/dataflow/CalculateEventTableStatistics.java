package com.latticeengines.prospectdiscovery.dataflow;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.metadata.Table;

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

        output = addDateRanges("AccountDateRangeBegin", "AccountDateRangeEnd", account, getSourceMetadata("Account"),
                output);

        // Contact stats
        output = addTotal("TotalContacts", contact, output);
        output = addDateRanges("ContactDateRangeBegin", "ContactDateRangeEnd", contact, getSourceMetadata("Contact"),
                output);

        // Lead stats
        output = addTotal("TotalLeads", lead, output);
        output = addDateRanges("LeadDateRangeBegin", "LeadDateRangeEnd", contact, getSourceMetadata("Lead"), output);

        // Opportunity stats
        output = addTotal("TotalOpportunities", opportunity, output);
        output = addTotal("TotalClosedOpportunities", opportunity, "IsClosed", //
                new FieldList("IsClosed"), output);
        output = addTotal("TotalClosedWonOpportunities", opportunity, "StageName.equals(\"Closed Won\")",
                new FieldList("StageName"), output);
        output = addDateRanges("OpportunityDateRangeBegin", "OpportunityDateRangeEnd", opportunity,
                getSourceMetadata("Opportunity"), output);
        return output;
    }

    private Node addTotal(String fieldName, Node source) {
        return addTotal(fieldName, source, null);
    }

    private Node addTotal(String fieldName, Node source, Node previous) {
        return addTotal(fieldName, source, null, null, previous);
    }

    private Node addTotal(String fieldName, Node source, String constraint, FieldList constraintFields, Node previous) {
        Node output;

        if (constraint != null && constraintFields != null) {
            source = source.filter(constraint, constraintFields);
        }
        String temporaryFieldName = String.format("__%s__", fieldName);

        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation("Id", temporaryFieldName, Aggregation.AggregationType.COUNT));
        Node aggregated = source.groupBy(new FieldList(), aggregations);
        if (previous != null) {
            output = previous.combine(aggregated);
        } else {
            output = aggregated;
        }

        // Deal with empty sets
        output = output.addFunction(String.format("%s == null ? new Long(0L) : %s", temporaryFieldName,
                temporaryFieldName), new FieldList(temporaryFieldName), new FieldMetadata(fieldName, Long.class));
        output = output.discard(new FieldList(temporaryFieldName));
        return output;
    }

    private Node addDateRanges(String minDateColumn, String maxDateColumn, Node source, Table sourceMetadata,
            Node previous) {
        String timestampField = sourceMetadata.getLastModifiedKey().getName();
        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation(timestampField, minDateColumn, Aggregation.AggregationType.MIN));
        previous = addAggregation(aggregations, source, previous);

        aggregations.clear();
        aggregations.add(new Aggregation(timestampField, maxDateColumn, Aggregation.AggregationType.MAX));
        previous = addAggregation(aggregations, source, previous);
        return previous;
    }

    private Node addAggregation(List<Aggregation> aggregations, Node source, Node previous) {
        source = source.groupBy(new FieldList(), aggregations);

        if (previous != null) {
            return previous.combine(source);
        }
        return source;
    }
}
