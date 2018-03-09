package com.latticeengines.cdl.workflow.steps.update;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;

@Component("cloneAccount")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CloneAccount extends BaseCloneEntityStep<ProcessAccountStepConfiguration> {

    @Override
    protected List<TableRoleInCollection> tablesToClone() {
        return Collections.emptyList();

    }

    @Override
    protected List<TableRoleInCollection> tablesToLink() {
        return Arrays.asList( //
                BusinessEntity.Account.getServingStore(), //
                TableRoleInCollection.Profile //
        );
    }

}
