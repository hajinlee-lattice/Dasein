package com.latticeengines.cdl.workflow.steps.rating;

import java.util.Arrays;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.CloneTableService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.GenerateRatingStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("cloneInactiveServingStores")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CloneInactiveServingStores extends BaseWorkflowStep<GenerateRatingStepConfiguration> {

    @Inject
    private CloneTableService cloneTableService;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Override
    public void execute() {
        CustomerSpace customerSpace = configuration.getCustomerSpace();
        DataCollection.Version active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        cloneTableService.setActiveVersion(active);
        cloneTableService.setCustomerSpace(customerSpace);
        Arrays.stream(BusinessEntity.values()).forEach(entity -> {
            TableRoleInCollection servingStore = entity.getServingStore();
            if (servingStore != null) {
                cloneTableService.linkInactiveTable(servingStore);
            }
        });
    }
}
