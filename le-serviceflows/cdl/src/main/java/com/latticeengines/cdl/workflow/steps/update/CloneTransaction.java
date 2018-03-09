package com.latticeengines.cdl.workflow.steps.update;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;

@Component("cloneTransaction")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CloneTransaction extends BaseCloneEntityStep<ProcessTransactionStepConfiguration> {

    @Override
    protected List<TableRoleInCollection> tablesToClone() {
        return Arrays.asList( //
                BusinessEntity.Transaction.getBatchStore(), //
                BusinessEntity.PeriodTransaction.getBatchStore() //
        );

    }

    @Override
    protected List<TableRoleInCollection> tablesToLink() {
        return Arrays.asList( //
                BusinessEntity.Transaction.getServingStore(), //
                BusinessEntity.PeriodTransaction.getServingStore() //
        );
    }

}
