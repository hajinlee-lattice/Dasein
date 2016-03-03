package com.latticeengines.prospectdiscovery.dataflow;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.serviceflows.dataflow.util.DataFlowUtils;

@Component("preMatchEventTableFlow")
public class PreMatchEventTableFlow extends TypesafeDataFlowBuilder<DataFlowParameters> {

    @Override
    public Node construct(DataFlowParameters parameters) {
        Node account = addSource("Account");
        Node contact = addSource("Contact");
        Node opportunity = addSource("Opportunity");
        Node stoplist = addSource("PublicDomain");

        Node removeNullEmailAddresses = contact.filter("Email != null && !Email.trim().isEmpty()", //
                new FieldList("Email"));

        Node removeNullAccountIds = removeNullEmailAddresses.filter( //
                "AccountId != null && !AccountId.trim().isEmpty()", new FieldList("AccountId"));

        Node retrieveDomains = removeNullAccountIds.addFunction( //
                "Email.substring(Email.indexOf('@') + 1)", //
                new FieldList("Email"), //
                new FieldMetadata("ContactDomain", String.class));
        retrieveDomains = DataFlowUtils.normalizeDomain(retrieveDomains, "ContactDomain");

        Node stopped = retrieveDomains.stopList(stoplist, "ContactDomain", "Domain");

        // Select domains with the largest number of entries for each account

        // Bucket into domains for each account
        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation("AccountId", "BucketSize", Aggregation.AggregationType.COUNT));
        Node retrieveDomainBucketsForEachAccount = stopped.groupBy(new FieldList("ContactDomain", "AccountId"), //
                aggregations);

        aggregations = new ArrayList<>();
        aggregations.add(new Aggregation("BucketSize", "MaxBucketSize", Aggregation.AggregationType.MAX));

        Node retrieveMaxDomainBucketSize = retrieveDomainBucketsForEachAccount.groupBy( //
                new FieldList("AccountId"), aggregations);

        retrieveMaxDomainBucketSize = retrieveMaxDomainBucketSize.renamePipe("RetrieveMaxDomainBucketSize");

        Node retrieveBestDomain = retrieveDomainBucketsForEachAccount.innerJoin( //
                new FieldList("AccountId", "BucketSize"), //
                retrieveMaxDomainBucketSize, //
                new FieldList("AccountId", "MaxBucketSize"));

        aggregations = new ArrayList<>();
        aggregations.add(new Aggregation("ContactDomain", "ContactDomain", Aggregation.AggregationType.MAX));
        Node resolveTies = retrieveBestDomain.groupBy(new FieldList("AccountId"), aggregations);

        Node joinedWithAccounts = account.leftOuterJoin(new FieldList("Id"), //
                resolveTies, //
                new FieldList("AccountId"));

        FieldMetadata domain = new FieldMetadata("Domain", String.class);
        domain.setPropertyValue("logicalType", "domain");
        domain.setPropertyValue("displayName", "Domain");
        Node domainsForEachAccount = joinedWithAccounts.addFunction(
                "Website != null && !Website.trim().isEmpty() ? Website : ContactDomain", //
                new FieldList("Website", "ContactDomain"), //
                domain);

        domainsForEachAccount = DataFlowUtils.normalizeDomain(domainsForEachAccount, "Domain");

        Node last = addIsWonEvent(domainsForEachAccount, opportunity);
        last = addStageClosedWonEvent(last, opportunity);
        last = addClosedEvent(last, opportunity);
        last = addOpportunityCreatedEvent(last, opportunity);

        // Add HasOpportunities
        last = last.addFunction( //
                "Event_OpportunityCreated", //
                new FieldList("Event_OpportunityCreated"), //
                new FieldMetadata("HasOpportunities", Boolean.class));

        last = addHasContacts(last, contact);

        List<String> accountSchema = account.getFieldNames();
        last = last.retain(new FieldList("Domain", "Event_IsWon", "Revenue_IsWon", //
                "Event_StageIsClosedWon", "Revenue_StageIsClosedWon", //
                "Event_IsClosed", "Revenue_IsClosed", "Event_OpportunityCreated", "Revenue_OpportunityCreated", //
                "HasContacts", "HasOpportunities") //
                .addAll(accountSchema));

        return last;
    }

    private Node addIsWonEvent(Node account, Node opportunity) {
        // Create a function for IsWon to convert to an integer
        FieldMetadata isWonInteger = new FieldMetadata("IsWonInteger", Integer.class);
        opportunity = opportunity.addFunction("IsWon ? 1 : 0", new FieldList("IsWon"), isWonInteger);

        // Create a function for Revenue predicated on IsWon = 1
        FieldMetadata revenue = new FieldMetadata("Revenue_IsWon", Double.class);
        opportunity = opportunity.addFunction("IsWon ? Amount : new Double(0.0)", new FieldList("IsWon", "Amount"),
                revenue);

        // Aggregate
        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation("IsWonInteger", "Count", Aggregation.AggregationType.SUM));
        aggregations.add(new Aggregation("Revenue_IsWon", "Revenue_IsWon", Aggregation.AggregationType.MAX));
        Node grouped = opportunity.groupBy(new FieldList("AccountId"), aggregations);

        // Left outer join with that
        Node joined = account.leftOuterJoin("Id", grouped, "AccountId");

        List<String> fieldsToRetain = account.getFieldNames();
        fieldsToRetain.add("Event_IsWon");
        fieldsToRetain.add("Revenue_IsWon");

        FieldMetadata event = getEventFieldMetadata("Event_IsWon");
        return joined //
                .addFunction("Count != null && Count > 0 ? true : false", new FieldList("Count"), event) //
                .checkpoint("addIsWonEvent") //
                .retain(new FieldList(fieldsToRetain));
    }

    private Node addStageClosedWonEvent(Node account, Node opportunity) {
        // Add Stage = "Closed Won" function
        opportunity = opportunity.addFunction( //
                "StageName.equals(\"Closed Won\") ? 1 : 0", //
                new FieldList("StageName"), //
                new FieldMetadata("StageIsClosedWon", Integer.class));

        // Create a function for Revenue predicated on Stage = "Closed Won"
        FieldMetadata revenue = new FieldMetadata("Revenue_StageIsClosedWon", Double.class);
        opportunity = opportunity.addFunction("StageName.equals(\"Closed Won\") ? Amount : new Double(0.0)",
                new FieldList("StageName", "Amount"), revenue);

        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation("StageIsClosedWon", "Count", Aggregation.AggregationType.SUM));
        aggregations.add(new Aggregation("Revenue_StageIsClosedWon", "Revenue_StageIsClosedWon",
                Aggregation.AggregationType.MAX));
        Node grouped = opportunity.groupBy(new FieldList("AccountId"), aggregations);

        Node joined = account.leftOuterJoin("Id", grouped, "AccountId");

        List<String> fieldsToRetain = account.getFieldNames();
        fieldsToRetain.add("Event_StageIsClosedWon");
        fieldsToRetain.add("Revenue_StageIsClosedWon");

        FieldMetadata event = getEventFieldMetadata("Event_StageIsClosedWon");
        return joined //
                .addFunction("Count != null && Count > 0 ? true : false", new FieldList("Count"), event) //
                .checkpoint("addStageClosedWonEvent") //
                .retain(new FieldList(fieldsToRetain));
    }

    private Node addClosedEvent(Node account, Node opportunity) {
        // Create a function for IsClosed to convert to an integer
        FieldMetadata isClosedInteger = new FieldMetadata("IsClosedInteger", Integer.class);
        opportunity = opportunity.addFunction("IsClosed ? 1 : 0", new FieldList("IsClosed"), isClosedInteger);

        // Create a function for Revenue predicated on IsClosed
        FieldMetadata revenue = new FieldMetadata("Revenue_IsClosed", Double.class);
        opportunity = opportunity.addFunction("IsClosed ? Amount : new Double(0.0)",
                new FieldList("IsClosed", "Amount"), revenue);

        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation("IsClosedInteger", "Count", Aggregation.AggregationType.SUM));
        aggregations.add(new Aggregation("Revenue_IsClosed", "Revenue_IsClosed", Aggregation.AggregationType.MAX));
        Node grouped = opportunity.groupBy(new FieldList("AccountId"), aggregations);

        Node joined = account.leftOuterJoin("Id", grouped, "AccountId");

        List<String> fieldsToRetain = account.getFieldNames();
        fieldsToRetain.add("Event_IsClosed");
        fieldsToRetain.add("Revenue_IsClosed");

        FieldMetadata event = getEventFieldMetadata("Event_IsClosed");
        return joined //
                .addFunction("Count != null && Count > 0 ? true : false", new FieldList("Count"), event) //
                .checkpoint("addClosedEvent") //
                .retain(new FieldList(fieldsToRetain));
    }

    private Node addOpportunityCreatedEvent(Node account, Node opportunity) {
        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation("AccountId", "Count", Aggregation.AggregationType.COUNT));
        aggregations.add(new Aggregation("Amount", "Revenue_OpportunityCreated", Aggregation.AggregationType.MAX));
        Node grouped = opportunity.groupBy(new FieldList("AccountId"), aggregations);

        Node joined = account.leftOuterJoin("Id", grouped, "AccountId");

        List<String> fieldsToRetain = account.getFieldNames();
        fieldsToRetain.add("Event_OpportunityCreated");
        fieldsToRetain.add("Revenue_OpportunityCreated");

        FieldMetadata event = getEventFieldMetadata("Event_OpportunityCreated");
        return joined //
                .addFunction("Count != null && Count > 0 ? true : false", new FieldList("Count"), event) //
                .checkpoint("addOpportunityCreatedEvent") //
                .retain(new FieldList(fieldsToRetain));
    }

    private Node addHasContacts(Node account, Node contact) {
        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation("AccountId", "Count", Aggregation.AggregationType.COUNT));
        Node grouped = contact.groupBy(new FieldList("AccountId"), aggregations);

        Node joined = account.leftOuterJoin("Id", grouped, "AccountId");

        List<String> fieldsToRetain = account.getFieldNames();
        fieldsToRetain.add("HasContacts");

        FieldMetadata event = new FieldMetadata("HasContacts", Boolean.class);
        event.setPropertyValue("displayName", "HasContacts");
        return joined //
                .addFunction("Count != null && Count > 0 ? true : false", new FieldList("Count"), event) //
                .checkpoint("addHasContacts") //
                .retain(new FieldList(fieldsToRetain));
    }

    private FieldMetadata getEventFieldMetadata(String column) {
        FieldMetadata event = new FieldMetadata(column, Boolean.class);
        event.setPropertyValue("LogicalType", "event");
        event.setPropertyValue("SemanticType", "Event");
        event.setPropertyValue("DisplayName", column);
        return event;
    }

}
