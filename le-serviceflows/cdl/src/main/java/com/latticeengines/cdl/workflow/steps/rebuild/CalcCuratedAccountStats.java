package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.BaseCalcStatsStep;
import com.latticeengines.domain.exposed.cdl.util.CuratedAttributeUtils;
import com.latticeengines.domain.exposed.datacloud.dataflow.stats.ProfileParameters;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.CuratedAccountAttributesStepConfiguration;

@Lazy
@Component("calcCuratedAccountStats")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CalcCuratedAccountStats extends BaseCalcStatsStep<CuratedAccountAttributesStepConfiguration> {

    @Override
    public void execute() {
        prepare();
        executeFullCalculation();
        if (hasKeyInContext(CURATED_ACCOUNT_SERVING_TABLE_NAME)) {
            Table table = getTableSummaryFromKey(customerSpace.toString(), CURATED_ACCOUNT_SERVING_TABLE_NAME);
            if (table != null) {
                exportTableRoleToRedshift(table, BusinessEntity.CuratedAccount.getServingStore());
            }
        }
    }

    @Override
    protected List<ProfileParameters.Attribute> getDeclaredAttrs() {
        List<ProfileParameters.Attribute> catAttrs = new ArrayList<>(
                CuratedAttributeUtils.getCategoricalAttributes(Account, getCreatedSourceNames()));
        catAttrs.addAll(CollectionUtils.emptyIfNull(super.getDeclaredAttrs()));
        return catAttrs;
    }

    @Override
    protected String getStatsTableCtxKey() {
        return CURATED_ACCOUNT_STATS_TABLE_NAME;
    }

}
