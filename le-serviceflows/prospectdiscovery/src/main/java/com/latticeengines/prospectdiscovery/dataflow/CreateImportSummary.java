package com.latticeengines.prospectdiscovery.dataflow;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.metadata.Table;

@Component("createImportSummary")
public class CreateImportSummary extends TypesafeDataFlowBuilder<DataFlowParameters> {
    public static final String TOTAL_ACCOUNTS = "TotalAccounts";
    public static final String TOTAL_ACCOUNTS_WITH_CONTACTS = "TotalAccountsWithContacts";
    public static final String TOTAL_ACCOUNTS_WITH_OPPORTUNITIES = "TotalAccountsWithOpportunities";
    public static final String TOTAL_UNIQUE_ACCOUNTS = "TotalUniqueAccounts";
    public static final String TOTAL_MATCHED_ACCOUNTS = "TotalMatchedAccounts";
    public static final String ACCOUNT_DATE_RANGE_BEGIN = "AccountDateRangeBegin";
    public static final String ACCOUNT_DATE_RANGE_END = "AccountDateRangeEnd";
    public static final String TOTAL_CONTACTS = "TotalContacts";
    public static final String CONTACT_DATE_RANGE_BEGIN = "ContactDateRangeBegin";
    public static final String CONTACT_DATE_RANGE_END = "ContactDateRangeEnd";
    public static final String LEAD_DATE_RANGE_BEGIN = "LeadDateRangeBegin";
    public static final String LEAD_DATE_RANGE_END = "LeadDateRangeEnd";
    public static final String TOTAL_OPPORTUNITIES = "TotalOpportunities";
    public static final String TOTAL_LEADS = "TotalLeads";
    public static final String TOTAL_CLOSED_OPPORTUNITIES = "TotalClosedOpportunities";
    public static final String TOTAL_CLOSED_WON_OPPORTUNITIES = "TotalClosedWonOpportunities";
    public static final String OPPORTUNITY_DATE_RANGE_BEGIN = "OpportunityDateRangeBegin";
    public static final String OPPORTUNITY_DATE_RANGE_END = "OpportunityDateRangeEnd";

    @Override
    public Node construct(DataFlowParameters parameters) {
        Node eventTable = addSource("EventTable");
        Node account = addSource("Account");
        Node opportunity = addSource("Opportunity");
        Node contact = addSource("Contact");
        Node lead = addSource("Lead");

        // Account stats
        Node output = addTotal(TOTAL_ACCOUNTS, account);
        output = addTotal(TOTAL_ACCOUNTS_WITH_CONTACTS, eventTable, //
                "HasContacts", new FieldList("HasContacts"), output);
        output = addTotal(TOTAL_ACCOUNTS_WITH_OPPORTUNITIES, eventTable, //
                "HasOpportunities", new FieldList("HasOpportunities"), output);
        output = addTotal(TOTAL_MATCHED_ACCOUNTS, eventTable, "IsMatched", new FieldList("IsMatched"), output);

        output = addTotal(TOTAL_UNIQUE_ACCOUNTS, eventTable, output);

        output = addDateRanges(ACCOUNT_DATE_RANGE_BEGIN, ACCOUNT_DATE_RANGE_END, account, getSourceMetadata("Account"),
                output);

        // Contact stats
        output = addTotal(TOTAL_CONTACTS, contact, output);
        output = addDateRanges(CONTACT_DATE_RANGE_BEGIN, CONTACT_DATE_RANGE_END, contact, getSourceMetadata("Contact"),
                output);

        // Lead stats
        output = addTotal(TOTAL_LEADS, lead, output);
        output = addDateRanges(LEAD_DATE_RANGE_BEGIN, LEAD_DATE_RANGE_END, contact, getSourceMetadata("Lead"), output);

        // Opportunity stats
        output = addTotal(TOTAL_OPPORTUNITIES, opportunity, output);
        output = addTotal(TOTAL_CLOSED_OPPORTUNITIES, opportunity, "IsClosed", //
                new FieldList("IsClosed"), output);
        output = addTotal(TOTAL_CLOSED_WON_OPPORTUNITIES, opportunity, "StageName.equals(\"Closed Won\")",
                new FieldList("StageName"), output);
        output = addDateRanges(OPPORTUNITY_DATE_RANGE_BEGIN, OPPORTUNITY_DATE_RANGE_END, opportunity,
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
