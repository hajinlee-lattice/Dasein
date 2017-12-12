package com.latticeengines.cdl.workflow.steps.update;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessStepConfiguration;

@Component("cloneContact")
public class CloneContact extends BaseCloneEntityStep<ProcessStepConfiguration> {

    @Override
    public List<TableRoleInCollection> tablesGeneratedViaMerge() {
        return Collections.singletonList(BusinessEntity.Contact.getBatchStore());
    }

    @Override
    protected List<TableRoleInCollection> tablesGeneratedViaRebuild() {
        return Arrays.asList( //
                BusinessEntity.Contact.getServingStore(), //
                TableRoleInCollection.ContactProfile //
        );
    }

    @Override
    public BusinessEntity importEntity() {
        return BusinessEntity.Contact;
    }

}
