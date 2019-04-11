package com.latticeengines.cdl.workflow.steps.update;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessContactStepConfiguration;

@Component
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MergeContactDiff extends BaseMergeTableRoleDiff<ProcessContactStepConfiguration> {

    @Override
    protected TableRoleInCollection getTableRole() {
        return BusinessEntity.Contact.getServingStore();
    }

}
