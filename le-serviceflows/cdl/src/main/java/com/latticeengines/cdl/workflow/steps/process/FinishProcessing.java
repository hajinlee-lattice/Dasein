package com.latticeengines.cdl.workflow.steps.process;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.SegmentProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("finishProcessing")
public class FinishProcessing extends BaseWorkflowStep<ProcessStepConfiguration> {

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private SegmentProxy segmentProxy;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Inject
    private EntityProxy entityProxy;

    private DataCollection.Version active;
    private DataCollection.Version inactive;
    private CustomerSpace customerSpace;

    @Override
    public void execute() {
        customerSpace = configuration.getCustomerSpace();
        active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);

        swapMissingTableRoles();

        log.info("Switch data collection to version " + inactive);
        dataCollectionProxy.switchVersion(customerSpace.toString(), inactive);
        try {
            // wait for local cache clean up
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            // ignore
        }
        // update segment and rating engine counts
        SegmentCountUtils.updateEntityCounts(segmentProxy, entityProxy, customerSpace.toString());
        RatingEngineCountUtils.updateRatingEngineCounts(ratingEngineProxy, customerSpace.toString());
    }

    private void swapMissingTableRoles() {
        for (TableRoleInCollection role: TableRoleInCollection.values()) {
            String inactiveTableName = dataCollectionProxy.getTableName(customerSpace.toString(), role, inactive);
            if (StringUtils.isBlank(inactiveTableName)) {
                String activeTableName = dataCollectionProxy.getTableName(customerSpace.toString(), role, active);
                if (StringUtils.isNotBlank(activeTableName)) {
                    log.info("Swapping table " + activeTableName + " from " + active + " to " + inactive + " as " + role);
                    dataCollectionProxy.unlinkTable(customerSpace.toString(), activeTableName, role, active);
                    dataCollectionProxy.upsertTable(customerSpace.toString(), activeTableName, role, inactive);
                }
            }
        }
    }

}
