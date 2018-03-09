package com.latticeengines.cdl.workflow.steps.reset;

import java.util.Arrays;
import java.util.Collection;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ResetTransaction extends BaseResetEntityStep<ProcessTransactionStepConfiguration> {

    protected Collection<BusinessEntity> getResettingEntities() {
        return Arrays.asList( //
                BusinessEntity.Transaction, //
                BusinessEntity.PeriodTransaction, //
                BusinessEntity.PurchaseHistory //
        );
    }

}
