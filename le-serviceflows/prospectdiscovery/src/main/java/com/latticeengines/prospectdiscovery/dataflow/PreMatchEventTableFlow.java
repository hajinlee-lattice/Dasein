package com.latticeengines.prospectdiscovery.dataflow;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

@Component("preMatchEventTableFlow")
public class PreMatchEventTableFlow extends TypesafeDataFlowBuilder<DataFlowParameters> {

    @Override
    public Node construct(DataFlowParameters parameters) {
        Node account = addSource("Account");
        Node contact = addSource("Contact");
        Node opportunity = addSource("Opportunity");
        Node stoplist = addSource("Stoplist");

        Node removeNullEmailAddresses = contact.filter("Email != null && !Email.trim().isEmpty()", //
                new FieldList("Email"));

        Node removeNullAccountIds = removeNullEmailAddresses.filter( //
                "AccountId != null && !AccountId.trim().isEmpty()", new FieldList("AccountId"));

        Node retrieveDomains = removeNullAccountIds.addFunction( //
                "Email.substring(Email.indexOf('@') + 1)", //
                new FieldList("Email"), //
                new FieldMetadata("ContactDomain", String.class));
        retrieveDomains = normalizeDomain(retrieveDomains, "ContactDomain");

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

        domainsForEachAccount = normalizeDomain(domainsForEachAccount, "Domain");

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
        last = last.retain(new FieldList("Domain", "Event_IsWon", "Event_StageIsClosedWon", //
                "Event_IsClosed", "Event_OpportunityCreated", "HasContacts", "HasOpportunities") //
                .addAll(accountSchema));

        last = last.rename( //
                new FieldList("BillingStreet", "BillingCity", "BillingState", "BillingCountry", "BillingPostalCode"), //
                new FieldList("Street", "City", "State", "Country", "PostalCode"));

        return last;
    }

    private Node addIsWonEvent(Node account, Node opportunity) {
        // Create a function for IsWon to convert to an integer
        FieldMetadata isWonInteger = new FieldMetadata("IsWonInteger", Integer.class);
        opportunity = opportunity.addFunction("IsWon ? 1 : 0", new FieldList("IsWon"), isWonInteger);

        // Get count of IsWonInteger for each account
        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation("IsWonInteger", "Count", Aggregation.AggregationType.SUM));
        Node grouped = opportunity.groupBy(new FieldList("AccountId"), aggregations);

        // Left outer join with that
        Node joined = account.leftOuterJoin("Id", grouped, "AccountId");

        List<String> fieldsToRetain = account.getFieldNames();
        fieldsToRetain.add("Event_IsWon");

        FieldMetadata event = new FieldMetadata("Event_IsWon", Boolean.class);
        event.setPropertyValue("logicalType", "event");
        event.setPropertyValue("displayName", "Event_IsWon");
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

        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation("StageIsClosedWon", "Count", Aggregation.AggregationType.SUM));
        Node grouped = opportunity.groupBy(new FieldList("AccountId"), aggregations);

        Node joined = account.leftOuterJoin("Id", grouped, "AccountId");

        List<String> fieldsToRetain = account.getFieldNames();
        fieldsToRetain.add("Event_StageIsClosedWon");

        FieldMetadata event = new FieldMetadata("Event_StageIsClosedWon", Boolean.class);
        event.setPropertyValue("logicalType", "event");
        event.setPropertyValue("displayName", "Event_StageIsClosedWon");
        return joined //
                .addFunction("Count != null && Count > 0 ? true : false", new FieldList("Count"), event) //
                .checkpoint("addStageClosedWonEvent") //
                .retain(new FieldList(fieldsToRetain));
    }

    private Node addClosedEvent(Node account, Node opportunity) {
        // Create a function for IsClosed to convert to an integer
        FieldMetadata isClosedInteger = new FieldMetadata("IsClosedInteger", Integer.class);
        opportunity = opportunity.addFunction("IsClosed ? 1 : 0", new FieldList("IsClosed"), isClosedInteger);

        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation("IsClosedInteger", "Count", Aggregation.AggregationType.SUM));
        Node grouped = opportunity.groupBy(new FieldList("AccountId"), aggregations);

        Node joined = account.leftOuterJoin("Id", grouped, "AccountId");

        List<String> fieldsToRetain = account.getFieldNames();
        fieldsToRetain.add("Event_IsClosed");

        FieldMetadata event = new FieldMetadata("Event_IsClosed", Boolean.class);
        event.setPropertyValue("logicalType", "event");
        event.setPropertyValue("displayName", "Event_IsClosed");
        return joined //
                .addFunction("Count != null && Count > 0 ? true : false", new FieldList("Count"), event) //
                .checkpoint("addClosedEvent") //
                .retain(new FieldList(fieldsToRetain));
    }

    private Node addOpportunityCreatedEvent(Node account, Node opportunity) {
        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation("AccountId", "Count", Aggregation.AggregationType.COUNT));
        Node grouped = opportunity.groupBy(new FieldList("AccountId"), aggregations);

        Node joined = account.leftOuterJoin("Id", grouped, "AccountId");

        List<String> fieldsToRetain = account.getFieldNames();
        fieldsToRetain.add("Event_OpportunityCreated");

        FieldMetadata event = new FieldMetadata("Event_OpportunityCreated", Boolean.class);
        event.setPropertyValue("logicalType", "event");
        event.setPropertyValue("displayName", "Event_OpportunityCreated");
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

    private Node normalizeDomain(Node last, String fieldName) {
        final String normalizeDomain = "%s != null ? %s.replaceAll(\"^http://\", \"\").replaceAll(\"^www[.]\", \"\") : null";

        return last.addFunction(String.format(normalizeDomain, fieldName, fieldName), new FieldList(fieldName),
                new FieldMetadata(fieldName, String.class));
    }
}
