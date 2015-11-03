package com.latticeengines.prospectdiscovery.dataflow;

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
        for (TargetMarket market : parameters.getTargetMarkets()) {
            Node marketAccounts = account.filter(market.getAccountFilter());
            Node intent = marketAccounts.filter(String.format("Score >= %f", market.getIntentScoreThreshold()),
                    new FieldList("Score"));
            // Node fit = marketAccounts.filter(String.format("Score >= %f",
            // market.getFitScoreThreshold()),
            // new FieldList("Score"));

            intent = intent.sort(market.getIntentSort());
            // fit = fit.sort("Score", false);
            intent = intent.innerJoin(new FieldList("Id"), contact, new FieldList("AccountId"));

            // fit = fit.innerJoin(new FieldList("Id"), contact, new
            // FieldList("AccountId"));

            // Remove intent generated prospects if they were generated more
            // than NumDaysBetweenIntentProspectResends days ago.
            String removeUnmatchedExpression = String.format("!(Email == null && %s != null)",
                    joinFieldName("ExistingProspect", "Email"));
            String dateExpression = String.format("%s == null || %dL - %s > %d",
                    joinFieldName("ExistingProspect", "CreatedDate"), //
                    DateTime.now().getMillis(), //
                    joinFieldName("ExistingProspect", "CreatedDate"), //
                    (long) market.getNumDaysBetweenIntentProspectResends() * 24 * 360 * 1000);
            intent = intent
                    .join(new FieldList("Email"), existingProspect, new FieldList("Email"), JoinType.OUTER)
                    .filter(removeUnmatchedExpression,
                            new FieldList("Email", joinFieldName("ExistingProspect", "Email"))) //
                    .filter(dateExpression, new FieldList(joinFieldName("ExistingProspect", "CreatedDate")));
            totals = intent;
        }
        return totals;
    }
}
