package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ContactProfile;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.BaseCalcStatsStep;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessContactStepConfiguration;

@Lazy
@Component("calcContactStats")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CalcContactStats extends BaseCalcStatsStep<ProcessContactStepConfiguration> {

    @Override
    public void execute() {
        prepare();
        autoDetectCategorical = true;
        autoDetectDiscrete = true;
        executeFullCalculation();
    }
    @Override
    protected TableRoleInCollection getProfileRole() {
        return ContactProfile;
    }

    @Override
    protected String getProfileTableCtxKey() {
        return CONTACT_PROFILE_TABLE_NAME;
    }

    @Override
    protected String getStatsTableCtxKey() {
        return CONTACT_STATS_TABLE_NAME;
    }

}
