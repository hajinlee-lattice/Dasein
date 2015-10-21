package com.latticeengines.prospectdiscovery.dataflow;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.CascadingDataFlowBuilder;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.metadata.Table;

@Component("createEventTable")
public class CreateEventTable extends CascadingDataFlowBuilder {

    @Override
    public Node constructFlowDefinition(DataFlowContext dataFlowCtx, Map<String, String> sources,
            Map<String, Table> sourceTables) {
        setDataFlowCtx(dataFlowCtx);
        Node account = addSource(sourceTables.get("Account"));
        Node contact = addSource(sourceTables.get("Contact"));
        Node opportunity = addSource(sourceTables.get("Opportunity"));
        Node stoplist = addSource(sourceTables.get("Stoplist"));

        Node removeNullEmailAddresses = contact.filter("Email != null && !Email.trim().isEmpty()", //
                new FieldList("Email"));

        Node removeNullAccountIds = removeNullEmailAddresses.filter( //
                "AccountId != null && !AccountId.trim().isEmpty()", new FieldList("AccountId"));

        FieldMetadata contactDomain = new FieldMetadata("ContactDomain", String.class);
        contactDomain.setPropertyValue("length", "255");
        contactDomain.setPropertyValue("logicalType", "domain");
        contactDomain.setPropertyValue("displayName", "ContactDomain");

        Node retrieveDomains = removeNullAccountIds.addFunction( //
                "Email.substring(Email.indexOf('@') + 1)", //
                new FieldList("Email"), //
                contactDomain);

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
        last = last.rename( //
                new FieldList("BillingCity", "BillingState", "BillingCountry"), //
                new FieldList("City", "State", "Country"));

        last = last.retain(new FieldList("Id", "Name", "City", "State", "Country", //
                "Domain", "Event_IsWon", "Event_StageIsClosedWon", "Event_IsClosed", //
                "Event_OpportunityCreated", "HasContacts", "HasOpportunities"));

        return last;
    }

    private Node addIsWonEvent(Node account, Node opportunity) {
        // Get count of IsWon for each account
        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation("IsWon", "Count", Aggregation.AggregationType.COUNT));
        Node grouped = opportunity.groupBy(new FieldList("AccountId"), aggregations);

        // Left outer join with that
        Node joined = account.leftOuterJoin("Id", grouped, "AccountId");

        List<String> fieldsToRetain = account.getFieldNames().getFieldsAsList();
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
                "StageName.equals(\"Closed Won\")", //
                new FieldList("StageName"), //
                new FieldMetadata("StageIsClosedWon", Boolean.class));

        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation("StageIsClosedWon", "Count", Aggregation.AggregationType.COUNT));
        Node grouped = opportunity.groupBy(new FieldList("AccountId"), aggregations);

        Node joined = account.leftOuterJoin("Id", grouped, "AccountId");

        List<String> fieldsToRetain = account.getFieldNames().getFieldsAsList();
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
        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation("IsClosed", "Count", Aggregation.AggregationType.COUNT));
        Node grouped = opportunity.groupBy(new FieldList("AccountId"), aggregations);

        Node joined = account.leftOuterJoin("Id", grouped, "AccountId");

        List<String> fieldsToRetain = account.getFieldNames().getFieldsAsList();
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

        List<String> fieldsToRetain = account.getFieldNames().getFieldsAsList();
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

        List<String> fieldsToRetain = account.getFieldNames().getFieldsAsList();
        fieldsToRetain.add("HasContacts");

        FieldMetadata event = new FieldMetadata("HasContacts", Boolean.class);
        event.setPropertyValue("displayName", "HasContacts");
        return joined //
                .addFunction("Count != null && Count > 0 ? true : false", new FieldList("Count"), event) //
                .checkpoint("addHasContacts") //
                .retain(new FieldList(fieldsToRetain));
    }

    public String constructFlowDefinition(DataFlowContext dataFlowContext, Map<String, String> sources) {
        return null;
    }
}
