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
    public String constructFlowDefinition(DataFlowContext dataFlowCtx, Map<String, String> sources, Map<String, Table> sourceTables) {
        setDataFlowCtx(dataFlowCtx);
        String account = addSource(sourceTables.get("Account"));
        String contact = addSource(sourceTables.get("Contact"));
        //String oppty = addSource(sourceTables.get("Opportunity"));
        String removeNullEmailAddresses = addFilter(contact, //
                "Email != null && !Email.trim().isEmpty()", //
                new FieldList("Email"));

        String removeNullAccountIds = addFilter(removeNullEmailAddresses,
                "AccountId != null && !AccountId.trim().isEmpty()",
                new FieldList("AccountId"));

        FieldMetadata domain = new FieldMetadata("Domain", String.class);
        domain.setPropertyValue("length", "255");
        domain.setPropertyValue("precision", "0");
        domain.setPropertyValue("scale", "0");
        domain.setPropertyValue("logicalType", "domain");
        domain.setPropertyValue("displayName", "Domain");

        String retrieveDomains = addFunction(removeNullAccountIds, //
                "Email.substring(Email.indexOf('@') + 1)", //
                new FieldList("Email"), //
                domain);

        // XXX Filter out public email address domains


        // Select domains with the largest number of entries for each account

        // Bucket into domains for each account
        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation("AccountId", "BucketSize", Aggregation.AggregationType.COUNT));
        String retrieveDomainBucketsForEachAccount = addGroupBy(retrieveDomains, //
                new FieldList("Domain", "AccountId"), //
                aggregations);

        aggregations = new ArrayList<>();
        aggregations.add(new Aggregation("BucketSize", "MaxBucketSize", Aggregation.AggregationType.MAX));

        String retrieveMaxDomainBucketSize = addGroupBy(retrieveDomainBucketsForEachAccount, //
                new FieldList("AccountId"), //
                aggregations);

        String retrieveBestDomain = addInnerJoin(retrieveDomainBucketsForEachAccount,
                new FieldList("AccountId", "BucketSize"), //
                retrieveMaxDomainBucketSize, //
                new FieldList("AccountId", "MaxBucketSize"));

        String joinedWithAccounts = addLeftOuterJoin(retrieveBestDomain, //
                new FieldList("AccountId"), //
                account, //
                new FieldList("AccountId"));

        // These domains may be null
        String bestDomainsForEachAccount = addFunction(
                joinedWithAccounts, //
                "Website != null && Website.trim().isEmpty ? Website : Domain", //
                new FieldList("Website", "Domain"), //
                domain);

        // Fill out HasOpportunities and HasContacts fields

        // Use event interpretation to set the event column using case when exists statement against opportunity


        return retrieveBestDomain;
    }

    public String constructFlowDefinition(DataFlowContext dataFlowContext, Map<String, String> sources) {
        return null;
    }
}
