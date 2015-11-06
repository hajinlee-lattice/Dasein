package com.latticeengines.prospectdiscovery.dataflow;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.query.ReferenceInterpretation;
import com.latticeengines.common.exposed.query.SingleReferenceLookup;
import com.latticeengines.common.exposed.query.Sort;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.domain.exposed.dataflow.flows.QuotaFlowParameters;
import com.latticeengines.domain.exposed.pls.ProspectDiscoveryConfiguration;
import com.latticeengines.domain.exposed.pls.ProspectDiscoveryOptionName;
import com.latticeengines.domain.exposed.pls.TargetMarket;

@Component("quotaFlow")
public class QuotaFlow extends TypesafeDataFlowBuilder<QuotaFlowParameters> {

    @Override
    public Node construct(QuotaFlowParameters parameters) {

        Node account = addSource("AccountMaster");
        Node contact = addSource("FilteredContact");
        Node existingProspect = addSource("ExistingProspect");
        Node scores = addSource("Scores");
        Node accountIntent = addSource("Intent");

        account = account.innerJoin("Id", accountIntent, "Id");

        TargetMarket market = parameters.getTargetMarket();
        ProspectDiscoveryConfiguration configuration = parameters.getConfiguration();

        account = account.filter(market.getAccountFilter());
        account = account.innerJoin("Id", scores, "Id");

        String scoreColumn = String.format("Score_%s", market.getModelId());

        Node intent = account.filter(String.format("%s >= %f", scoreColumn, market.getIntentScoreThreshold()), //
                new FieldList(scoreColumn));
        Node fit = account.filter(String.format("%s >= %f", scoreColumn, market.getFitScoreThreshold()), //
                new FieldList(scoreColumn));

        intent = intent.renamePipe("Intent");
        fit = fit.renamePipe("Fit");

        intent = intent.filter( //
                getIntentExistsExpression(market.getIntentSort()), getFields(market.getIntentSort()));

        intent = intent.innerJoin(new FieldList("Id"), contact, new FieldList("AccountId"));
        fit = fit.innerJoin(new FieldList("Id"), contact, new FieldList("AccountId"));

        intent = removePreviouslyGeneratedIntent(intent, market, existingProspect);
        fit = removePreviouslyGeneratedFit(fit, existingProspect);

        intent = intent.addFunction("true", new FieldList(), new FieldMetadata("IsIntent", Boolean.class));
        fit = fit.addFunction("false", new FieldList(), new FieldMetadata("IsIntent", Boolean.class));

        Node merged = intent.merge(fit);

        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation("IsIntent", "IsIntent", Aggregation.AggregationType.MAX));
        aggregations.add(new Aggregation("AccountId", "AccountId", Aggregation.AggregationType.MAX));
        Node deduped = merged //
                .groupBy(new FieldList("Email"), aggregations) //
                .filter("Email != null", new FieldList("Email"));

        // Reconsitute account columns
        Node joined = deduped.innerJoin("AccountId", account, "Id");

        if (!market.isDeliverProspectsFromExistingAccounts()) {
            Node existingAccount = addSource("ExistingAccount");
            joined = joined.stopList(existingAccount, "Id", "Id");
        }

        // Split again
        intent = joined.filter("IsIntent", new FieldList("IsIntent")).renamePipe("Intent");
        fit = joined.filter("!IsIntent", new FieldList("IsIntent")).renamePipe("Fit");

        intent = intent.sort(market.getIntentSort());
        fit = fit.sort(fitSort(market));

        intent = intent.addRowID("DeliveryOffset");
        fit = fit.addRowID("DeliveryOffset");

        double intentRatio = configuration.getDouble(ProspectDiscoveryOptionName.IntentPercentage, 50.0) / 100.0;
        intent = intent.limit((int) (intentRatio * (double) market.getNumProspectsDesired()));
        fit = fit.limit((int) ((1.0 - intentRatio) * (double) market.getNumProspectsDesired()));

        merged = intent.merge(fit);

        merged = merged //
                .addFunction(market.getOffset().toString(), new FieldList(), new FieldMetadata("MarketOffset",
                        Integer.class));

        merged = merged.sort("DeliveryOffset");

        return merged.retain(new FieldList("Email", "IsIntent", "Id", "Score", "MarketOffset", "DeliveryOffset"));
    }

    private Node removePreviouslyGeneratedIntent(Node intent, TargetMarket market, Node existingProspect) {
        // Remove intent generated prospects if they were generated more
        // than NumDaysBetweenIntentProspectResends days ago.
        String removeUnmatchedExpression = String.format("!(Email == null && %s != null)",
                joinFieldName("ExistingProspect", "Email"));
        String dateExpression = String.format("%s == null || %dL - %s > %dL",
                joinFieldName("ExistingProspect", "CreatedDate"), //
                DateTime.now().getMillis(), //
                joinFieldName("ExistingProspect", "CreatedDate"), //
                (long) market.getNumDaysBetweenIntentProspectResends() * 24 * 360 * 1000);
        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation("Score", "Score", Aggregation.AggregationType.MAX));
        aggregations.add(new Aggregation("AccountId", "AccountId", Aggregation.AggregationType.MAX));
        return intent.join(new FieldList("Email"), existingProspect, new FieldList("Email"), JoinType.OUTER)
                .filter(removeUnmatchedExpression, new FieldList("Email", joinFieldName("ExistingProspect", "Email")))
                .filter(dateExpression, new FieldList(joinFieldName("ExistingProspect", "CreatedDate")))
                .groupBy(new FieldList("Email"), aggregations);
    }

    private Node removePreviouslyGeneratedFit(Node fit, Node existingProspect) {
        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation("Score", "Score", Aggregation.AggregationType.MAX));
        aggregations.add(new Aggregation("AccountId", "AccountId", Aggregation.AggregationType.MAX));
        return fit.stopList(existingProspect, "Email", "Email") //
                .groupBy(new FieldList("Email"), aggregations);
    }

    private Sort fitSort(TargetMarket market) {
        SingleReferenceLookup lookup = new SingleReferenceLookup(String.format("Score_%s", market.getModelId()),
                ReferenceInterpretation.COLUMN);
        List<SingleReferenceLookup> lookups = new ArrayList<>();
        lookups.add(lookup);
        return new Sort(lookups, true);
    }

    private String getIntentExistsExpression(Sort intent) {
        // TODO use zoltan
        StringBuilder sb = new StringBuilder();
        int size = intent.getLookups().size();
        if (size == 0) {
            return "true";
        }
        for (int i = 0; i < size; ++i) {
            SingleReferenceLookup lookup = intent.getLookups().get(i);
            sb.append(lookup.getReference().toString());
            sb.append(" != ");
            sb.append("0");
            if (i != size - 1) {
                sb.append(" && ");
            }
        }
        return sb.toString();
    }

    private FieldList getFields(Sort sort) {
        List<String> fields = new ArrayList<>();
        for (SingleReferenceLookup lookup : sort.getLookups()) {
            fields.add(lookup.getReference().toString());
        }
        return new FieldList(fields);
    }
}
