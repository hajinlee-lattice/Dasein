package com.latticeengines.cdl.workflow.steps.process;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.CloneTableService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("finishProcessing")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class FinishProcessing extends BaseWorkflowStep<ProcessStepConfiguration> {

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private SegmentProxy segmentProxy;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Inject
    private EntityProxy entityProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private CloneTableService cloneTableService;

    private DataCollection.Version active;
    private DataCollection.Version inactive;
    private CustomerSpace customerSpace;

    @Override
    public void execute() {
        customerSpace = configuration.getCustomerSpace();
        active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);

        deleteOrphanTables();
        swapMissingTableRoles();

        log.info("Switch data collection to version " + inactive);
        dataCollectionProxy.switchVersion(customerSpace.toString(), inactive);
        log.info("Evict attr repo cache for inactive version " + inactive);
        dataCollectionProxy.evictAttrRepoCache(customerSpace.toString(), inactive);
        if (StringUtils.isNotBlank(configuration.getDataCloudBuildNumber())) {
            dataCollectionProxy.updateDataCloudBuildNumber(customerSpace.toString(),
                    configuration.getDataCloudBuildNumber());
        }
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
        cloneTableService.setCustomerSpace(customerSpace);
        cloneTableService.setActiveVersion(active);
        Set<BusinessEntity> resetEntities = getSetObjectFromContext(RESET_ENTITIES, BusinessEntity.class);
        if (resetEntities == null) {
            resetEntities = Collections.emptySet();
        }
        for (TableRoleInCollection role : TableRoleInCollection.values()) {
            BusinessEntity ownerEntity = getOwnerEntity(role);
            if (ownerEntity != null && resetEntities.contains(ownerEntity)) {
                // skip swap for reset entities
                continue;
            }
            cloneTableService.linkInactiveTable(role);
        }
    }

    private void deleteOrphanTables() {
        cleanupEntityTableMap(getMapObjectFromContext(ENTITY_DIFF_TABLES, BusinessEntity.class, String.class));
        cleanupEntityTableMap(getMapObjectFromContext(TABLE_GOING_TO_REDSHIFT, BusinessEntity.class, String.class));
    }

    private void cleanupEntityTableMap(Map<BusinessEntity, String> entityTableNames) {
        if (MapUtils.isNotEmpty(entityTableNames)) {
            entityTableNames.forEach((entity, tableName) -> {
                String servingStoreName = dataCollectionProxy.getTableName(customerSpace.toString(),
                        entity.getServingStore(), inactive);
                if (!tableName.equals(servingStoreName)) {
                    log.info("Removing orphan table " + tableName);
                    metadataProxy.deleteTable(customerSpace.toString(), tableName);
                }
            });
        }
    }

    private BusinessEntity getOwnerEntity(TableRoleInCollection role) {
        BusinessEntity owner = Arrays.stream(BusinessEntity.values()).filter(entity -> //
        role.equals(entity.getBatchStore()) || role.equals(entity.getServingStore())) //
                .findFirst().orElse(null);
        if (owner == null) {
            switch (role) {
            case Profile:
                return BusinessEntity.Account;
            case ContactProfile:
                return BusinessEntity.Contact;
            case PurchaseHistoryProfile:
                return BusinessEntity.PurchaseHistory;
            case ConsolidatedRawTransaction:
                return BusinessEntity.Transaction;
            default:
                return null;
            }
        }
        return owner;
    }

}
