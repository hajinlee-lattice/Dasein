package com.latticeengines.cdl.workflow.steps.export;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.EaiJobConfiguration;
import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.export.ExportDataToRedshiftConfiguration;
import com.latticeengines.proxy.exposed.eai.EaiProxy;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.redshiftdb.exposed.utils.RedshiftUtils;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("exportDataToRedshift")
public class ExportDataToRedshift extends BaseWorkflowStep<ExportDataToRedshiftConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ExportDataToRedshift.class);

    @Inject
    private EaiProxy eaiProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    private Map<BusinessEntity, Table> entityTableMap;
    private Map<BusinessEntity, Boolean> appendFlagMap;

    private String customerSpace;

    @Override
    public void execute() {
        log.info("Inside ExportData execute()");
        customerSpace = configuration.getCustomerSpace().toString();

        Map<BusinessEntity, String> entityTableNames = getMapObjectFromContext(TABLE_GOING_TO_REDSHIFT,
                BusinessEntity.class, String.class);
        entityTableMap = new HashMap<>();
        if (entityTableNames != null) {
            entityTableNames.forEach((entity, tableName) -> {
                Table table = metadataProxy.getTable(customerSpace, tableName);
                entityTableMap.put(entity, table);
            });
        }
        if (entityTableMap.isEmpty()) {
            entityTableMap = configuration.getSourceTables();
            putObjectInContext(TABLE_GOING_TO_REDSHIFT, entityTableMap);
        }

        if (entityTableMap.isEmpty()) {
            log.info("No table to export, skip this step.");
            return;
        }

        Map<BusinessEntity, Long> exportReportMap = new HashMap<>();
        entityTableMap.forEach((entity, table) -> {
            Long count = table.getExtracts().get(0).getProcessedRecords();
            exportReportMap.put(entity, count);
        });
        putObjectInContext(REDSHIFT_EXPORT_REPORT, exportReportMap);

        appendFlagMap = getMapObjectFromContext(APPEND_TO_REDSHIFT_TABLE, BusinessEntity.class, Boolean.class);
        if (appendFlagMap == null) {
            appendFlagMap = configuration.getAppendFlagMap();
        }
        ExecutorService executors = ThreadPoolUtils.getFixedSizeThreadPool("redshift-export", entityTableMap.size());
        List<Future<?>> futures = new ArrayList<>();
        for (Map.Entry<BusinessEntity, Table> entry : entityTableMap.entrySet()) {
            boolean createNew = !appendFlagMap.getOrDefault(entry.getKey(), false);
            Exporter exporter = new Exporter(createNew, entry.getKey(), entry.getValue());
            futures.add(executors.submit(exporter));
        }
        long startTime = System.currentTimeMillis();
        while (!futures.isEmpty() && System.currentTimeMillis() - startTime < TimeUnit.DAYS.toMillis(1)) {
            List<Future<?>> finishedFutures = new ArrayList<>();
            futures.forEach(future -> {
                try {
                    future.get(1, TimeUnit.MINUTES);
                    finishedFutures.add(future);
                } catch (TimeoutException e) {
                    // ignore
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException("Failed to wait for the EAI job to finish.", e);
                }
            });
            futures.removeAll(finishedFutures);
        }
    }

    private class Exporter implements Runnable {

        private final BusinessEntity entity;
        private final Table table;

        private boolean createNew;
        private DataCollection.Version targetVersion;

        Exporter(boolean createNew, BusinessEntity entity, Table table) {
            this.createNew = createNew;
            this.entity = entity;
            this.table = table;
        }

        @Override
        public void run() {
            String targetName = renameTable(entity, table);
            log.info(
                    "Uploading to redshift table " + targetName + " for entity " + entity + ", createNew=" + createNew);
            exportData(table, targetName, entity.getServingStore());
            entityTableMap.put(entity, table);
            if (createNew) {
                log.info("Upsert " + entity.getServingStore() + " table " + table.getName() + " at version "
                        + targetVersion);
                dataCollectionProxy.upsertTable(customerSpace, table.getName(), entity.getServingStore(),
                        targetVersion);
            }
        }

        private void exportData(Table sourceTable, String targetTableName, TableRoleInCollection tableRole) {
            EaiJobConfiguration exportConfig = setupExportConfig(sourceTable, targetTableName, tableRole);
            log.info(String.format("The export config is\n%s", JsonUtils.pprint(exportConfig)));
            AppSubmission submission = eaiProxy.submitEaiJob(exportConfig);
            putStringValueInContext(EXPORT_DATA_APPLICATION_ID, submission.getApplicationIds().get(0));
            waitForAppId(submission.getApplicationIds().get(0));
        }

        private String renameTable(BusinessEntity entity, Table table) {
            String goodName;
            if (StringUtils.isNotBlank(configuration.getTargetTableName())) {
                log.info("Enforce target table name to be " + configuration.getTargetTableName());
                goodName = configuration.getTargetTableName();
            } else {
                String prefix = String.join("_", CustomerSpace.parse(customerSpace).getTenantId(), entity.name());
                goodName = NamingUtils.timestamp(prefix);
                log.info("Generated a new target table name: " + goodName);
            }
            DataCollection.Version inactiveVersion = dataCollectionProxy.getInactiveVersion(customerSpace);
            if (createNew) {
                // create for inactive version
                targetVersion = inactiveVersion;
            } else {
                // upserting to active version
                Table oldTable = dataCollectionProxy.getTable(customerSpace, entity.getServingStore(),
                        inactiveVersion.complement());
                if (oldTable == null) {
                    log.warn("Table for " + entity.getServingStore() + " at version " + inactiveVersion.complement()
                            + " does not exists. Switch to not append.");
                    createNew = true; // it is essentially creating new
                    // create for active version
                    targetVersion = inactiveVersion.complement();
                } else {
                    goodName = oldTable.getName();
                }
            }
            if (createNew) {
                log.info("Renaming table " + table.getName() + " to " + goodName);
                metadataProxy.updateTable(customerSpace, goodName, table);
                table.setName(goodName);
            }
            return goodName;
        }

        private ExportConfiguration setupExportConfig(Table sourceTable, String targetTableName,
                TableRoleInCollection tableRole) {
            // all distributed on account id
            String distKey = tableRole.getPrimaryKey().name();
            List<String> sortKeys = new ArrayList<>(tableRole.getForeignKeysAsStringList());
            if (!sortKeys.contains(tableRole.getPrimaryKey().name())) {
                sortKeys.add(tableRole.getPrimaryKey().name());
            }
            RedshiftTableConfiguration.SortKeyType sortKeyType = sortKeys.size() == 1
                    ? RedshiftTableConfiguration.SortKeyType.Compound
                    : RedshiftTableConfiguration.SortKeyType.Interleaved;

            HdfsToRedshiftConfiguration exportConfig;
            if (configuration.getHdfsToRedshiftConfiguration() != null) {
                exportConfig = JsonUtils.deserialize(
                        JsonUtils.serialize(configuration.getHdfsToRedshiftConfiguration()),
                        HdfsToRedshiftConfiguration.class);
            } else {
                throw new IllegalStateException("Cannot find HdfsToRedshiftConfiguration in step configuration.");
            }
            exportConfig.setCustomerSpace(CustomerSpace.parse(customerSpace));
            exportConfig.setExportInputPath(sourceTable.getExtractsDirectory() + "/*.avro");
            exportConfig.setExportTargetPath(sourceTable.getName());
            exportConfig.setNoSplit(true);
            exportConfig.setExportDestination(ExportDestination.REDSHIFT);
            if (createNew) {
                exportConfig.setCreateNew(true);
                exportConfig.setAppend(true);
            } else {
                exportConfig.setCreateNew(false);
                exportConfig.setAppend(false);
            }
            RedshiftTableConfiguration redshiftTableConfig = exportConfig.getRedshiftTableConfiguration();
            redshiftTableConfig.setDistStyle(RedshiftTableConfiguration.DistStyle.Key);
            redshiftTableConfig.setDistKey(distKey);
            redshiftTableConfig.setSortKeyType(sortKeyType);
            redshiftTableConfig.setSortKeys(sortKeys);
            redshiftTableConfig.setTableName(targetTableName);
            redshiftTableConfig.setJsonPathPrefix(
                    String.format("%s/jsonpath/%s.jsonpath", RedshiftUtils.AVRO_STAGE, targetTableName));
            return exportConfig;
        }
    }

}
