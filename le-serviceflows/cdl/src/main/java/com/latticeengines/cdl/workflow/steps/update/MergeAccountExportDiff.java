package com.latticeengines.cdl.workflow.steps.update;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;

@Component
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MergeAccountExportDiff extends BaseMergeTableRoleDiff<ProcessAccountStepConfiguration> {

    @Override
    protected TableRoleInCollection getTableRole() {
        return TableRoleInCollection.AccountExport;
    }

    @Override
    protected boolean publishToRedshift() {
        return false;
    }

    @Override
    protected String getJoinKey() {
        return InterfaceName.AccountId.name();
    }

    @Override
    protected boolean saveParquet() {
        return true;
    }

    @Override
    protected boolean failOnMissingMasterTable() {
        return false;
    }

}
