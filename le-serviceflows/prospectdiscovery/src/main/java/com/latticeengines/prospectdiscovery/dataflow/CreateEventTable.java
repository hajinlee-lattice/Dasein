package com.latticeengines.prospectdiscovery.dataflow;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.metadata.Table;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.CascadingDataFlowBuilder;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

@Component("createEventTable")
public class CreateEventTable extends CascadingDataFlowBuilder {

    @Override
    public Node constructFlowDefinition(DataFlowContext dataFlowCtx, Map<String, String> sources, Map<String, Table> sourceTables) {
        setDataFlowCtx(dataFlowCtx);
        Node account = addSource(sourceTables.get("Account"));
        Node contact = addSource(sourceTables.get("Contact"));
        Node opportunity = addSource(sourceTables.get("Opportunity"));
        Node removeNullEmailAddresses = contact.filter("Email != null && !Email.trim().isEmpty()", //
                new FieldList("Email"));

        Node removeNullAccountIds = removeNullEmailAddresses.filter("AccountId != null && !AccountId.trim().isEmpty()",
                new FieldList("AccountId"));

        // XXX Remove duplicate Account Ids (otherwise counts will be off)

        FieldMetadata contactDomain = new FieldMetadata("ContactDomain", String.class);
        contactDomain.setPropertyValue("length", "255");
        contactDomain.setPropertyValue("precision", "0");
        contactDomain.setPropertyValue("scale", "0");
        contactDomain.setPropertyValue("logicalType", "domain");
        contactDomain.setPropertyValue("displayName", "ContactDomain");

        Node retrieveDomains = removeNullAccountIds.addFunction(
                "Email.substring(Email.indexOf('@') + 1)", //
                new FieldList("Email"), //
                contactDomain);

        // XXX Filter out public email address domains

        // Select domains with the largest number of entries for each account

        // Bucket into domains for each account
        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation("AccountId", "BucketSize", Aggregation.AggregationType.COUNT));
        Node retrieveDomainBucketsForEachAccount = retrieveDomains.groupBy(new FieldList("ContactDomain", "AccountId"), //
                aggregations);

        aggregations = new ArrayList<>();
        aggregations.add(new Aggregation("BucketSize", "MaxBucketSize", Aggregation.AggregationType.MAX));

        Node retrieveMaxDomainBucketSize = retrieveDomainBucketsForEachAccount.groupBy(
                new FieldList("AccountId"), aggregations);

        retrieveMaxDomainBucketSize = retrieveMaxDomainBucketSize.renamePipe("RetrieveMaxDomainBucketSize");

        Node retrieveBestDomain = retrieveDomainBucketsForEachAccount.innerJoin(
                new FieldList("AccountId", "BucketSize"), //
                retrieveMaxDomainBucketSize, //
                new FieldList("AccountId", "MaxBucketSize"));

        aggregations = new ArrayList<>();
        aggregations.add(new Aggregation("ContactDomain", "ContactDomain", Aggregation.AggregationType.MAX));
        Node resolveTies = retrieveBestDomain.groupBy(new FieldList("AccountId"), aggregations);

        Node joinedWithAccounts = account.leftOuterJoin(
                new FieldList("Id"), //
                resolveTies, //
                new FieldList("AccountId"));

        // These domains may be null
        FieldMetadata domain = new FieldMetadata("Domain", String.class);
        domain.setPropertyValue("length", "255");
        domain.setPropertyValue("precision", "0");
        domain.setPropertyValue("scale", "0");
        domain.setPropertyValue("logicalType", "domain");
        domain.setPropertyValue("displayName", "Domain");
        Node domainsForEachAccount = joinedWithAccounts.addFunction(
                "Website != null && !Website.trim().isEmpty() ? Website : ContactDomain", //
                new FieldList("Website", "ContactDomain"), //
                domain);

        // XXX Remove "NULL"s
        
        // XXX Handle multiple different events/filters

    	// Get count of IsWon for each account
        aggregations = new ArrayList<>();
        aggregations.add(new Aggregation("IsWon", "WinCount", Aggregation.AggregationType.COUNT));
        Node wins = opportunity.groupBy(new FieldList("AccountId"), //
                aggregations);
        		
        // Left outer join with that
        Node joinedWithWins = domainsForEachAccount.leftOuterJoin(
                new FieldList("Id"), //
                wins, //
                new FieldList("AccountId"));
        
        FieldMetadata event = new FieldMetadata("Event", Boolean.class);
        event.setPropertyValue("logicalType", "event");
        event.setPropertyValue("displayName", "Event");
        Node retrieveEventColumn = joinedWithWins.addFunction(
                "WinCount != null && WinCount > 0 ? true : false",
                new FieldList("WinCount"),
                event);

/*
        String completed = addRetainFunction(
        		retrieveEventColumn, 
        		null);
*/
        return retrieveEventColumn;
    }

    public String constructFlowDefinition(DataFlowContext dataFlowContext, Map<String, String> sources) {
        return null;
    }
}
