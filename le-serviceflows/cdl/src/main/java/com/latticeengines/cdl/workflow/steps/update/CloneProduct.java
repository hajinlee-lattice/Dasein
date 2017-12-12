package com.latticeengines.cdl.workflow.steps.update;

import java.util.Collections;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessStepConfiguration;

@Component("cloneProduct")
public class CloneProduct extends BaseCloneEntityStep<ProcessStepConfiguration> {

    @Override
    protected List<TableRoleInCollection> tablesGeneratedViaRebuild() {
        return Collections.singletonList(BusinessEntity.Product.getServingStore());
    }

    @Override
    public List<TableRoleInCollection> tablesGeneratedViaMerge() {
        return Collections.singletonList(BusinessEntity.Product.getBatchStore());
    }

    @Override
    public BusinessEntity importEntity() {
        return BusinessEntity.Product;
    }

}
