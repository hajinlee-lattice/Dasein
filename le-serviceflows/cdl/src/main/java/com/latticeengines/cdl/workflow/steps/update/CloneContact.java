package com.latticeengines.cdl.workflow.steps.update;

import java.util.Arrays;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessStepConfiguration;

@Component("cloneContact")
public class CloneContact extends BaseCloneEntityStep<ProcessStepConfiguration> {

    @Override
    protected List<TableRoleInCollection> tablesToClone() {
        return Arrays.asList( //
                BusinessEntity.Contact.getBatchStore(), //
                BusinessEntity.Contact.getServingStore(), //
                TableRoleInCollection.ContactProfile //
        );
    }

}
