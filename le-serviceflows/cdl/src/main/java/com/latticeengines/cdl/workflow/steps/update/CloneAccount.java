package com.latticeengines.cdl.workflow.steps.update;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessStepConfiguration;

@Component("cloneAccount")
public class CloneAccount extends BaseCloneEntityStep<ProcessStepConfiguration> {

    @Override
    public List<TableRoleInCollection> tablesGeneratedViaMerge() {
        return Collections.singletonList(BusinessEntity.Account.getBatchStore());
    }

    @Override
    protected List<TableRoleInCollection> tablesGeneratedViaRebuild() {
        return Arrays.asList( //
                BusinessEntity.Account.getServingStore(), //
                TableRoleInCollection.Profile //
        );
    }

    @Override
    public BusinessEntity importEntity() {
        return BusinessEntity.Account;
    }

}
