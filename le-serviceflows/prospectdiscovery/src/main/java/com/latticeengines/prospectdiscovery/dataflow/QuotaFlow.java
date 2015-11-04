package com.latticeengines.prospectdiscovery.dataflow;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.domain.exposed.dataflow.flows.QuotaFlowParameters;
import com.latticeengines.domain.exposed.pls.TargetMarket;

@Component("quotaFlow")
public class QuotaFlow extends TypesafeDataFlowBuilder<QuotaFlowParameters> {

    @Override
    public Node construct(QuotaFlowParameters parameters) {
        Node account = addSource("ScoredAccount");
        Node contact = addSource("FilteredContact");
        Node existingProspect = addSource("ExistingProspect");
        // Node existingAccount = addSource("ExistingAccount");

        Node totals = null;
        // TODO order target markets
        for (TargetMarket market : parameters.getTargetMarkets()) {
            Node marketAccounts = account.filter(market.getAccountFilter());
            Node intent = marketAccounts.filter(String.format("Score >= %f", market.getIntentScoreThreshold()),
                    new FieldList("Score"));
            Node fit = marketAccounts.filter(String.format("Score >= %f", market.getFitScoreThreshold()),
                    new FieldList("Score"));
            intent = intent.renamePipe("Intent");
            fit = fit.renamePipe("Fit");

            intent = intent.sort(market.getIntentSort());
            fit = fit.sort("Score", false);
            intent = intent.innerJoin(new FieldList("Id"), contact, new FieldList("AccountId"));

            fit = fit.innerJoin(new FieldList("Id"), contact, new FieldList("AccountId"));

            intent = removePreviouslyGeneratedIntent(intent, market, existingProspect);
            fit = removePreviouslyGeneratedFit(fit, existingProspect);

            Node merged = intent.join(new FieldList("Email"), fit, new FieldList("Email"), JoinType.LEFT);
            totals = merged;
        }
        totals = totals.retain(new FieldList("Email", "Score"));
        return totals;
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
        return intent.join(new FieldList("Email"), existingProspect, new FieldList("Email"), JoinType.OUTER)
                .filter(removeUnmatchedExpression, new FieldList("Email", joinFieldName("ExistingProspect", "Email")))
                .filter(dateExpression, new FieldList(joinFieldName("ExistingProspect", "CreatedDate")))
                .groupBy(new FieldList("Email"), aggregations);
    }

    private Node removePreviouslyGeneratedFit(Node fit, Node existingProspect) {
        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation("Score", "Score", Aggregation.AggregationType.MAX));
        return fit.stopList(existingProspect, "Email", "Email") //
                .groupBy(new FieldList("Email"), aggregations);
    }
}
