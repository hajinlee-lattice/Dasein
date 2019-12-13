package com.latticeengines.cdl.workflow.steps.validations;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BaseProcessEntityStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.util.SparkUtils;
import com.latticeengines.spark.exposed.service.LivySessionService;
import com.latticeengines.spark.exposed.service.SparkJobService;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

public abstract class BaseValidateBatchStore<T extends BaseProcessEntityStepConfiguration> extends BaseWorkflowStep<T> {

    private static final Logger log = LoggerFactory.getLogger(BaseValidateBatchStore.class);

    @Inject
    protected DataCollectionProxy dataCollectionProxy;

    @Inject
    protected MetadataProxy metadataProxy;

    @Inject
    private LivySessionService sessionService;

    @Inject
    private SparkJobService sparkJobService;

    protected DataCollection.Version inactive;
    protected CustomerSpace customerSpace;
    protected BusinessEntity entity;

    @Override
    public void execute() {
        customerSpace = configuration.getCustomerSpace();
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        entity = configuration.getMainEntity();
        validate();
        postProcessing();
    }

    private void validate() {
        TableRoleInCollection role = BusinessEntity.Transaction.equals(entity) ?
                TableRoleInCollection.ConsolidatedRawTransaction : entity.getBatchStore();

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
        String hdfsPath = table.getExtracts().get(0).getPath();
        if (!hdfsPath.endsWith("*.avro")) {
            if (hdfsPath.endsWith("/")) {
                hdfsPath += "*.avro";
            } else {
                hdfsPath += "/*.avro";
            }
        }
        log.info("Count records in HDFS " + hdfsPath);
        Long result = SparkUtils.countRecordsInGlobs(sessionService, sparkJobService, yarnConfiguration, hdfsPath);
        log.info(String.format("Table role %s version %s has %d entities.", tableRole.name(), version.name(), result));
        return result;
    }

    protected abstract void postProcessing();
}
