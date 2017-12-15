package com.latticeengines.cdl.workflow.steps.update;

import java.util.Collections;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessProductStepConfiguration;

@Component("cloneProduct")
public class CloneProduct extends BaseCloneEntityStep<ProcessProductStepConfiguration> {

    @Override
    protected List<TableRoleInCollection> tablesToClone() {
        return Collections.singletonList(BusinessEntity.Product.getServingStore());
    }

}
