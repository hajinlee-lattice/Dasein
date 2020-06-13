package com.latticeengines.cdl.workflow.steps.validations;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.cdl.workflow.steps.BaseProcessAnalyzeSparkStep;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BaseProcessEntityStepConfiguration;
import com.latticeengines.serviceflows.workflow.util.SparkUtils;
import com.latticeengines.spark.exposed.service.LivySessionService;
import com.latticeengines.spark.exposed.service.SparkJobService;

public abstract class BaseValidateBatchStore<T extends BaseProcessEntityStepConfiguration> extends BaseProcessAnalyzeSparkStep<T> {

    private static final Logger log = LoggerFactory.getLogger(BaseValidateBatchStore.class);

    @Inject
    private LivySessionService sessionService;

    @Inject
    private SparkJobService sparkJobService;

    protected BusinessEntity entity;
    private TableRoleInCollection role;

    @Override
    public void execute() {
        bootstrap();
        entity = configuration.getMainEntity();
        role = BusinessEntity.Transaction.equals(entity) ? //
                TableRoleInCollection.ConsolidatedRawTransaction : entity.getBatchStore();
        validate();
        syncBatchStoreOrResetServingStore();
    }

    private void syncBatchStoreOrResetServingStore() {
        String activeName = dataCollectionProxy.getTableName(customerSpace.toString(), role, active);
        String inactiveName = dataCollectionProxy.getTableName(customerSpace.toString(), role, inactive);
        if (StringUtils.isNotBlank(activeName) && StringUtils.isBlank(inactiveName)) {
            linkInactiveTable(role);
        } else if (StringUtils.isBlank(activeName) && StringUtils.isBlank(inactiveName)) {
            log.info("No {} batch store in both versions, going to reset its serving store", entity);
            updateEntitySetInContext(RESET_ENTITIES, entity);
        }
    }

    private void validate() {
        if (batchStoreExists(role, inactive)) {
            long totalRecords = countRawEntitiesInHdfs(role, inactive);
            if (totalRecords <= 0L) {
                throw new LedpException(LedpCode.LEDP_18239, new String[] {entity.name()});
            }
        }
    }

    private boolean batchStoreExists(TableRoleInCollection tableRole, DataCollection.Version version) {
        String tableName = dataCollectionProxy.getTableName(customerSpace.toString(), tableRole, version);
        if (StringUtils.isBlank(tableName)) {
            return false;
        }
        Table table = metadataProxy.getTable(customerSpace.toString(), tableName);
        return table != null;
    }

    private long countRawEntitiesInHdfs(TableRoleInCollection tableRole, DataCollection.Version version) {
        String tableName = dataCollectionProxy.getTableName(customerSpace.toString(), tableRole, version);
        if (StringUtils.isBlank(tableName)) {
            return 0L;
        }
        Table table = metadataProxy.getTable(customerSpace.toString(), tableName);
        if (table == null) {
            log.error("Cannot find table " + tableName);
            return 0L;
        }
        Long count = table.getExtracts().get(0).getProcessedRecords();
        if (count == null || count <= 0) {
            String hdfsPath = table.getExtracts().get(0).getPath();
            hdfsPath = PathUtils.toAvroGlob(hdfsPath);
            log.info("Count records in HDFS " + hdfsPath);
            count = SparkUtils.countRecordsInGlobs(sessionService, sparkJobService, yarnConfiguration, hdfsPath);
        }
        log.info(String.format("Table role %s version %s has %d entities.", tableRole.name(), version.name(), count));
        return count;
    }

}
