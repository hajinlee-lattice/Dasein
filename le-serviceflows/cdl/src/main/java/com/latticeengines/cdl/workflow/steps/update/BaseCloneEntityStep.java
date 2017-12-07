package com.latticeengines.cdl.workflow.steps.update;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessStepConfiguration;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.security.exposed.util.MultiTenantContext;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

public abstract class BaseCloneEntityStep<T extends ProcessStepConfiguration> extends BaseWorkflowStep<T> {

    @Inject
    private RedshiftService redshiftService;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private MetadataProxy metadataProxy;

    private DataCollection.Version active;
    private DataCollection.Version inactive;
    private CustomerSpace customerSpace;

    @Value("${dataplatform.queue.scheme}")
    private String queueScheme;

    @Override
    public void execute() {
        customerSpace = configuration.getCustomerSpace();
        active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);

        for (TableRoleInCollection role: tablesToClone()) {
            String activeTableName = dataCollectionProxy.getTableName(customerSpace.toString(), role, active);
            if (StringUtils.isNotBlank(activeTableName)) {
                cloneToInactiveTable(role);
            }
        }

    }
    private void cloneToInactiveTable(TableRoleInCollection role) {
        log.info("Cloning " + role + " from  " + active + " to " + inactive);
        String activeTableName = dataCollectionProxy.getTableName(customerSpace.toString(), role, active);
        Table activeTable = dataCollectionProxy.getTable(customerSpace.toString(), role, active);
        String cloneName = NamingUtils.timestamp(role.name());
        Table inactiveTable = JsonUtils.clone(activeTable);
        inactiveTable.setExtracts(null);
        inactiveTable.setTableType(TableType.DATATABLE);
        inactiveTable.setName(cloneName);
        inactiveTable = cloneDataTable(activeTable, inactiveTable);
        if (isServingStore(role)) {
            copyRedshiftTable(activeTableName, cloneName);
        }
        metadataProxy.createTable(customerSpace.toString(), cloneName, inactiveTable);
        dataCollectionProxy.upsertTable(customerSpace.toString(), cloneName, role, inactive);
    }

    private void copyRedshiftTable(String original, String clone) {
        redshiftService.dropTable(clone);
        redshiftService.cloneTable(original, clone);
    }

    private Table cloneDataTable(Table original, Table clone) {
        String cloneDataPath = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(),
                customerSpace, original.getNamespace()).append(clone.getName()).toString();

        Extract newExtract = new Extract();
        newExtract.setPath(cloneDataPath + "/*.avro");
        newExtract.setName(NamingUtils.uuid("Extract"));
        AtomicLong count = new AtomicLong(0);
        if (original.getExtracts() != null && original.getExtracts().size() > 0) {
            try {
                if (HdfsUtils.fileExists(yarnConfiguration, cloneDataPath)) {
                    HdfsUtils.rmdir(yarnConfiguration, cloneDataPath);
                }
                HdfsUtils.mkdir(yarnConfiguration, cloneDataPath);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create table dir at " + cloneDataPath);
            }

            original.getExtracts().forEach(extract -> {
                String srcPath = extract.getPath();
                if (!srcPath.endsWith(".avro") && !srcPath.endsWith("*.avro")) {
                    srcPath = srcPath.endsWith("/") ? srcPath : srcPath + "/";
                    srcPath += "*.avro";
                }
                String srcDir = srcPath.substring(0, srcPath.lastIndexOf("/"));
                try {
                    String queue = LedpQueueAssigner.getPropDataQueueNameForSubmission();
                    queue = LedpQueueAssigner.overwriteQueueAssignment(queue, queueScheme);
                    log.info(String.format("Copying table data from %s to %s", srcDir, cloneDataPath));
                    HdfsUtils.distcp(yarnConfiguration, srcDir, cloneDataPath, queue);
                } catch (Exception e) {
                    throw new RuntimeException(String.format("Failed to copy in HDFS from %s to %s", srcPath,
                            cloneDataPath), e);
                }

                if (extract.getProcessedRecords() != null && extract.getProcessedRecords() > 0) {
                    count.addAndGet(extract.getProcessedRecords());
                }
            });
        }
        newExtract.setProcessedRecords(count.get());
        newExtract.setExtractionTimestamp(System.currentTimeMillis());
        clone.setExtracts(Collections.singletonList(newExtract));

        String oldTableSchema = PathBuilder.buildDataTableSchemaPath(CamilleEnvironment.getPodId(),
                MultiTenantContext.getCustomerSpace(), original.getNamespace()).append(original.getName()).toString();
        String cloneTableSchema = oldTableSchema.replace(original.getName(), clone.getName());

        try {
            if (HdfsUtils.fileExists(yarnConfiguration, oldTableSchema)) {
                HdfsUtils.copyFiles(yarnConfiguration, oldTableSchema, cloneTableSchema);
                log.info(String.format("Copying table schema from %s to %s", oldTableSchema, cloneTableSchema));
            }
        } catch (IOException e) {
            throw new RuntimeException(String.format("Failed to copy schema in HDFS from %s to %s", oldTableSchema,
                    cloneTableSchema), e);
        }

        metadataProxy.createTable(customerSpace.toString(), clone.getName(), clone);
        return metadataProxy.getTable(customerSpace.toString(), clone.getName());
    }

    private boolean isServingStore(TableRoleInCollection role) {
        for (BusinessEntity entity: BusinessEntity.values()) {
            if (role.equals(entity.getServingStore())) {
                return true;
            }
        }
        return false;
    }

    protected abstract List<TableRoleInCollection> tablesToClone();

}
