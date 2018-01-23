package com.latticeengines.cdl.workflow.steps.update;

import java.util.Arrays;
import java.util.List;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component("cloneAccount")
public class CloneAccount extends BaseCloneEntityStep<ProcessAccountStepConfiguration> {

    @Override
    protected List<TableRoleInCollection> tablesToClone() {
        return Arrays.asList( //
                BusinessEntity.Account.getServingStore(), //
                TableRoleInCollection.Profile //
        );
    }

}
