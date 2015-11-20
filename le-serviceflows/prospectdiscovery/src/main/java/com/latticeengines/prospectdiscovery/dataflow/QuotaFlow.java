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
import com.latticeengines.domain.exposed.pls.TargetMarketDataFlowConfiguration;
import com.latticeengines.domain.exposed.pls.TargetMarketDataFlowOptionName;

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
        TargetMarketDataFlowConfiguration marketConfiguration = market.getDataFlowConfiguration();

        account = account.innerJoin(LatticeAccountID, scores, LatticeAccountID);

        String scoreColumn = String.format("Score_%s", market.getModelId());

        IntentScore intentScoreThreshold = IntentScore.valueOf( //
                marketConfiguration.getString(TargetMarketDataFlowOptionName.IntentScoreThreshold));
        double fitScoreThreshold = marketConfiguration.getDouble(TargetMarketDataFlowOptionName.FitScoreThreshold);

        Node intent = account.filter( //
                getIntentAboveThresholdExpression(market.getSelectedIntent(), intentScoreThreshold), //
                new FieldList(market.getSelectedIntent()));
        Node fit = account.filter(String.format("%s >= %f", scoreColumn, fitScoreThreshold), //
                new FieldList(scoreColumn));

        intent = intent.renamePipe("Intent");
        fit = fit.renamePipe("Fit");

        intent = intent.filter( //
                getIntentExistsExpression(market.getSelectedIntent()), new FieldList(market.getSelectedIntent()));

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

        if (!marketConfiguration.getBoolean(TargetMarketDataFlowOptionName.DeliverProspectsFromExistingAccounts, true)) {
            joined = joined.stopList(existingProspect, LatticeAccountID, LatticeAccountID);
        }

        // Split again
        intent = joined.filter("IsIntent", new FieldList("IsIntent")).renamePipe("Intent");
        fit = joined.filter("!IsIntent", new FieldList("IsIntent")).renamePipe("Fit");

        intent = sortByIntent(intent, market.getSelectedIntent());
        fit = sortByFit(fit, market.getModelId());

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

        Integer maxProspectsPerAccount = marketConfiguration.get(TargetMarketDataFlowOptionName.MaxProspectsPerAccount,
                null, Integer.class);
        if (maxProspectsPerAccount != null) {
            merged = merged.groupByAndLimit(new FieldList(LatticeAccountID), maxProspectsPerAccount);
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

        TargetMarketDataFlowConfiguration dataFlowConfiguration = market.getDataFlowConfiguration();
        Integer numDaysBetweenResends = dataFlowConfiguration.get(
                TargetMarketDataFlowOptionName.NumDaysBetweenIntentProspecResends, null, Integer.class);

        if (numDaysBetweenResends != null) {
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
                    (long) numDaysBetweenResends * 24 * 360 * 1000);
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

    private Node sortByFit(Node last, String modelId) {
        SingleReferenceLookup lookup = new SingleReferenceLookup(String.format("Score_%s", modelId),
                ReferenceInterpretation.COLUMN);
        List<SingleReferenceLookup> lookups = new ArrayList<>();
        lookups.add(lookup);
        Sort sort = new Sort(lookups, true);
        return last.sort(sort);
    }

    private Node sortByIntent(Node last, List<String> intent) {
        if (intent.size() == 0)
            return last;
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        for (int i = 0; i < intent.size(); ++i) {
            sb.append(intent.get(i));
            if (i < intent.size() - 1) {
                sb.append(" + ");
            }
        }
        sb.append(String.format(") / %dF", intent.size()));

        String averageExpression = sb.toString();
        last = last.addFunction(averageExpression, new FieldList(intent), new FieldMetadata("AverageIntent",
                Double.class));

        last = last.sort("AverageIntent", true);
        last = last.discard(new FieldList("AverageIntent"));
        return last;
    }

    private String getIntentExistsExpression(List<String> intent) {
        StringBuilder sb = new StringBuilder();
        int size = intent.size();
        if (size == 0) {
            return "true";
        }
        for (int i = 0; i < size; ++i) {
            String lookup = intent.get(i);
            sb.append(lookup);
            sb.append(" != 0 && ");
            sb.append(lookup);
            sb.append(" != null");
            if (i != size - 1) {
                sb.append(" && ");
            }

        }
        return sb.toString();
    }

    private String getIntentAboveThresholdExpression(List<String> intent, IntentScore threshold) {
        StringBuilder sb = new StringBuilder();
        int size = intent.size();
        if (size == 0) {
            return "true";
        }
        for (int i = 0; i < size; ++i) {
            String lookup = intent.get(i);
            sb.append(lookup);
            sb.append(String.format(" >= %d && ", threshold.getValue()));
            sb.append(lookup);
            sb.append(" != null");
            if (i != size - 1) {
                sb.append(" && ");
            }

        }
        return sb.toString();
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

        if (market.getSelectedIntent() == null) {
            throw new IllegalArgumentException(String.format("Intent must be set for market with name %s",
                    market.getName()));
        }

        if (market.getSelectedIntent().size() == 0) {
            throw new IllegalArgumentException(String.format(
                    "Must provide at least one selected intent field for market with name %s", market.getName()));
        }
    }
}
