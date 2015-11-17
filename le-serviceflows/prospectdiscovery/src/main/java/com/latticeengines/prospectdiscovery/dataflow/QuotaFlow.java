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
import com.latticeengines.domain.exposed.pls.IntentScore;
import com.latticeengines.domain.exposed.pls.ProspectDiscoveryConfiguration;
import com.latticeengines.domain.exposed.pls.ProspectDiscoveryOptionName;
import com.latticeengines.domain.exposed.pls.Quota;
import com.latticeengines.domain.exposed.pls.TargetMarket;

@Component("quotaFlow")
public class QuotaFlow extends TypesafeDataFlowBuilder<QuotaFlowParameters> {
    public static final String LatticeAccountID = "LatticeAccountID";
    public static final String DeliveryDate = "DeliveryDate";

    @Override
    public Node construct(QuotaFlowParameters parameters) {

        Node account = addSource("AccountMaster");
        Node contact = addSource("FilteredContact");
        Node existingProspect = addSource("ExistingProspect");
        Node scores = addSource("Scores");
        Node accountIntent = addSource("Intent");

        account = account.leftOuterJoin(LatticeAccountID, accountIntent, LatticeAccountID);

        TargetMarket market = parameters.getTargetMarket();
        ProspectDiscoveryConfiguration configuration = parameters.getConfiguration();
        Quota quota = parameters.getQuota();

        account = account.innerJoin(LatticeAccountID, scores, LatticeAccountID);

        String scoreColumn = String.format("Score_%s", market.getModelId());

        IntentScore intentScoreThreshold = market.getIntentScoreThreshold();
        double fitScoreThreshold = market.getFitScoreThreshold() != null ? market.getFitScoreThreshold() : 0;

        Node intent = account.filter(getIntentAboveThresholdExpression(market.getIntentSort(), intentScoreThreshold),
                getFields(market.getIntentSort()));
        Node fit = account.filter(String.format("%s >= %f", scoreColumn, fitScoreThreshold), //
                new FieldList(scoreColumn));

        intent = intent.renamePipe("Intent");
        fit = fit.renamePipe("Fit");

        intent = intent.filter( //
                getIntentExistsExpression(market.getIntentSort()), getFields(market.getIntentSort()));

        intent = intent.innerJoin(new FieldList(LatticeAccountID), contact, new FieldList(LatticeAccountID));
        fit = fit.innerJoin(new FieldList(LatticeAccountID), contact, new FieldList(LatticeAccountID));

        intent = removePreviouslyGeneratedIntent(intent, market, existingProspect);
        fit = removePreviouslyGeneratedFit(fit, existingProspect);

        intent = intent.addFunction("true", new FieldList(), new FieldMetadata("IsIntent", Boolean.class));
        fit = fit.addFunction("false", new FieldList(), new FieldMetadata("IsIntent", Boolean.class));

        Node merged = intent.merge(fit);

        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation("IsIntent", "IsIntent", Aggregation.AggregationType.MAX));
        aggregations.add(new Aggregation(LatticeAccountID, LatticeAccountID, Aggregation.AggregationType.MAX));
        Node deduped = merged //
                .groupBy(new FieldList("Email"), aggregations) //
                .filter("Email != null", new FieldList("Email"));

        // Reconsitute account columns
        Node joined = deduped.innerJoin(LatticeAccountID, account, LatticeAccountID);

        if (!market.isDeliverProspectsFromExistingAccounts()) {
            joined = joined.stopList(existingProspect, LatticeAccountID, LatticeAccountID);
        }

        // Split again
        intent = joined.filter("IsIntent", new FieldList("IsIntent")).renamePipe("Intent");
        fit = joined.filter("!IsIntent", new FieldList("IsIntent")).renamePipe("Fit");

        intent = intent.sort(market.getIntentSort());
        fit = fit.sort(fitSort(market));

        intent = intent.addRowID("Offset");
        fit = fit.addRowID("Offset");

        if (market.getNumProspectsDesired() != null) {
            double intentRatio = configuration.getDouble(ProspectDiscoveryOptionName.IntentPercentage, 50.0) / 100.0;
            intent = intent.limit((int) (intentRatio * (double) market.getNumProspectsDesired()));
            fit = fit.limit((int) ((1.0 - intentRatio) * (double) market.getNumProspectsDesired()));
        }

        merged = intent.merge(fit);

        if (merged.getFieldNames().contains(DeliveryDate)) {
            merged = merged.discard(new FieldList(DeliveryDate));
        }
        merged = merged.addFunction(String.format("%dL", DateTime.now().getMillis()), new FieldList(),
                new FieldMetadata(DeliveryDate, Long.class));

        if (market.getMaxProspectsPerAccount() != null) {
            merged = merged.groupByAndLimit(new FieldList(LatticeAccountID), market.getMaxProspectsPerAccount());
        }

        merged = merged.sort("Offset");
        merged = merged.limit(quota.getBalance());

        merged = merged.rename(new FieldList(scoreColumn), new FieldList("Score"));
        merged = merged.retain(new FieldList("Email", "IsIntent", LatticeAccountID, "Score", DeliveryDate));

        return merged;
    }

    private Node removePreviouslyGeneratedIntent(Node intent, TargetMarket market, Node existingProspect) {
        // Remove intent generated prospects if they were generated more
        // than NumDaysBetweenIntentProspectResends days ago.

        if (market.getNumDaysBetweenIntentProspectResends() != null) {
            String removeUnmatchedExpression = String.format("!(Email == null && %s != null)",
                    joinFieldName("ExistingProspect", "Email"));
            intent = intent //
                    .join(new FieldList("Email"), existingProspect, new FieldList("Email"), JoinType.OUTER) //
                    .filter(removeUnmatchedExpression,
                            new FieldList("Email", joinFieldName("ExistingProspect", "Email")));

            String dateExpression = String.format("%s == null || %dL - %s > %dL", //
                    DeliveryDate, //
                    DateTime.now().getMillis(), //
                    DeliveryDate, //
                    (long) market.getNumDaysBetweenIntentProspectResends() * 24 * 360 * 1000);
            intent = intent.filter(dateExpression, new FieldList(DeliveryDate));
        } else {
            intent = intent.stopList(existingProspect, "Email", "Email");
        }

        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation(LatticeAccountID, LatticeAccountID, Aggregation.AggregationType.MAX));
        return intent.groupBy(new FieldList("Email"), aggregations);
    }

    private Node removePreviouslyGeneratedFit(Node fit, Node existingProspect) {
        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation(LatticeAccountID, LatticeAccountID, Aggregation.AggregationType.MAX));
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
        StringBuilder sb = new StringBuilder();
        int size = intent.getLookups().size();
        if (size == 0) {
            return "true";
        }
        for (int i = 0; i < size; ++i) {
            SingleReferenceLookup lookup = intent.getLookups().get(i);
            sb.append(lookup.getReference().toString());
            sb.append(" != 0 && ");
            sb.append(lookup.getReference().toString());
            sb.append(" != null");
            if (i != size - 1) {
                sb.append(" && ");
            }

        }
        return sb.toString();
    }

    private String getIntentAboveThresholdExpression(Sort intent, IntentScore threshold) {
        StringBuilder sb = new StringBuilder();
        int size = intent.getLookups().size();
        if (size == 0) {
            return "true";
        }
        for (int i = 0; i < size; ++i) {
            SingleReferenceLookup lookup = intent.getLookups().get(i);
            sb.append(lookup.getReference().toString());
            sb.append(String.format(" >= %d && ", threshold.getValue()));
            sb.append(lookup.getReference().toString());
            sb.append(" != null");
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

    @Override
    public void validate(QuotaFlowParameters parameters) {
        TargetMarket market = parameters.getTargetMarket();
        if (market.getModelId() == null) {
            throw new IllegalArgumentException(String.format("ModelID must be set for market with name %s",
                    market.getName()));
        }

        if (market.getOffset() == null) {
            throw new IllegalArgumentException(String.format("Market offset must be set for market with name %s",
                    market.getName()));
        }

        if (market.getIntentSort() == null) {
            throw new IllegalArgumentException(String.format("Intent sort must be set for market with name %s",
                    market.getName()));
        }

        if (market.isDeliverProspectsFromExistingAccounts() == null) {
            throw new IllegalArgumentException(String.format(
                    "isDeliverProspectsFromExistingAccounts must be set for market with name %s", market.getName()));
        }
    }
}
