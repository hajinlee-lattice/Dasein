package com.latticeengines.cdl.workflow.steps.rebuild;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.BaseCalcStatsStep;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.CuratedAccountAttributesStepConfiguration;

@Lazy
@Component("calcCuratedAccountStats")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CalcCuratedAccountStats extends BaseCalcStatsStep<CuratedAccountAttributesStepConfiguration> {

    @Override
    public void execute() {
        prepare();
        executeFullCalculation();
    }

    @Override
    protected String getStatsTableCtxKey() {
        return CURATED_ACCOUNT_STATS_TABLE_NAME;
    }

}
