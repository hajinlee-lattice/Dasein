package com.latticeengines.cdl.workflow.steps.process;

import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessStepConfiguration;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.metadata.SegmentProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.proxy.exposed.objectapi.RatingProxy;
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

    @Inject
    private RatingProxy ratingProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private Configuration yarnConfiguration;

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
        registerEntityCountReport();

        log.info("Switch data collection to version " + inactive);
        dataCollectionProxy.switchVersion(customerSpace.toString(), inactive);
        log.info("Evict attr repo cache for inactive version " + inactive);
        dataCollectionProxy.evictAttrRepoCache(customerSpace.toString(), inactive);
        if (StringUtils.isNotBlank(configuration.getDataCloudVersion())) {
            dataCollectionProxy.updateDataCloudVersion(customerSpace.toString(), configuration.getDataCloudVersion());
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

    private void registerEntityCountReport() {
        long accountNumber = countInRedshift(BusinessEntity.Account);
        long contactNumber = countInRedshift(BusinessEntity.Contact);
        long transactionNumber = countRawTransactionInHdfs();
        ObjectNode json = JsonUtils.createObjectNode();
        json.put(BusinessEntity.Account.name(), accountNumber);
        json.put(BusinessEntity.Contact.name(), contactNumber);
        json.put(BusinessEntity.Transaction.name(), transactionNumber);
        Report report = createReport(json.toString(), ReportPurpose.ENTITY_NUMBER_SUMMARY,
                UUID.randomUUID().toString());
        registerReport(configuration.getCustomerSpace(), report);
    }

    private long countRawTransactionInHdfs() {
        String rawTableName = dataCollectionProxy.getTableName(customerSpace.toString(),
                TableRoleInCollection.ConsolidatedRawTransaction, inactive);
        log.info(String.format("Found raw transaction table in inactive version %s", inactive));
        Table rawTable = metadataProxy.getTable(customerSpace.toString(), rawTableName);
        if (rawTable == null) {
            log.warn("Cannot find raw transaction table.");
            return 0l;
        }
        return AvroUtils.count(yarnConfiguration, rawTable.getExtracts().get(0).getPath());
    }

    private long countInRedshift(BusinessEntity entity) {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setMainEntity(entity);
        return ratingProxy.getCountFromObjectApi(customerSpace.toString(), frontEndQuery, inactive);
    }

    private void swapMissingTableRoles() {
        for (TableRoleInCollection role : TableRoleInCollection.values()) {
            String inactiveTableName = dataCollectionProxy.getTableName(customerSpace.toString(), role, inactive);
            if (StringUtils.isBlank(inactiveTableName)) {
                String activeTableName = dataCollectionProxy.getTableName(customerSpace.toString(), role, active);
                if (StringUtils.isNotBlank(activeTableName)) {
                    log.info("Swapping table " + activeTableName + " from " + active + " to " + inactive + " as "
                            + role);
                    dataCollectionProxy.unlinkTable(customerSpace.toString(), activeTableName, role, active);
                    dataCollectionProxy.upsertTable(customerSpace.toString(), activeTableName, role, inactive);
                }
            }
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
                if (StringUtils.isBlank(servingStoreName)) {
                    // TODO: always keep rating serving store for now
                    if (!BusinessEntity.Rating.equals(entity)) {
                        log.info("Removing orphan table " + tableName);
                        metadataProxy.deleteTable(customerSpace.toString(), tableName);
                    }
                }
            });
        }
    }

}
