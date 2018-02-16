package com.latticeengines.cdl.workflow.steps.update;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;

@Component("clonePurchaseHistory")
public class ClonePurchaseHistory extends BaseCloneEntityStep<ProcessTransactionStepConfiguration> {

    @Override
    protected List<TableRoleInCollection> tablesToClone() {
        return Collections.emptyList();

    }

    @Override
    protected List<TableRoleInCollection> tablesToLink() {
        return Arrays.asList( //
                BusinessEntity.PurchaseHistory.getServingStore(), //
                TableRoleInCollection.PurchaseHistoryProfile //
        );
    }

}
