package com.latticeengines.cdl.workflow.steps.update;

import java.util.Arrays;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessStepConfiguration;

@Component("cloneProduct")
public class CloneProduct extends BaseCloneEntityStep<ProcessStepConfiguration> {

    @Override
    protected List<TableRoleInCollection> tablesToClone() {
        return Arrays.asList( //
                BusinessEntity.Product.getBatchStore(), //
                BusinessEntity.Product.getServingStore() //
        );
    }

}
