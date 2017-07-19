package com.latticeengines.cdl.workflow.steps.export;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.eai.EaiJobConfiguration;
import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.export.ExportDataToRedshiftConfiguration;
import com.latticeengines.proxy.exposed.eai.EaiProxy;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;
import com.latticeengines.redshiftdb.exposed.utils.RedshiftUtils;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("exportDataToRedshift")
public class ExportDataToRedshift extends BaseWorkflowStep<ExportDataToRedshiftConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ExportDataToRedshift.class);

    @Autowired
    private EaiProxy eaiProxy;

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @Autowired
    private RedshiftService redshiftService;

    private Map<BusinessEntity, Table> entityTableMap;

    private Map<BusinessEntity, Boolean> appendTableMap;

    @Override
    public void execute() {
        log.info("Inside ExportData execute()");
        entityTableMap = getMapObjectFromContext(TABLE_GOING_TO_REDSHIFT, BusinessEntity.class, Table.class);
        appendTableMap = getMapObjectFromContext(APPEND_TO_REDSHIFT_TABLE, BusinessEntity.class, Boolean.class);
        if (entityTableMap == null) {
            entityTableMap = configuration.getSourceTables();
        }
        if (appendTableMap == null) {
            appendTableMap = new HashMap<>();
        }
        for (Map.Entry<BusinessEntity, Table> entry : entityTableMap.entrySet()) {
            boolean append = Boolean.TRUE.equals(appendTableMap.get(entry.getKey()));
            String targetName = renameTable(entry, append);
            log.info("Uploading to redshift table " + targetName + ", append=" + append);
            exportData(entry.getValue(), targetName, entry.getKey().getServingStore(), append);
        }
    }

    @Override
    public void onExecutionCompleted() {
        for (Map.Entry<BusinessEntity, Table> entry : entityTableMap.entrySet()) {
            BusinessEntity entity = entry.getKey();
            boolean append = Boolean.TRUE.equals(appendTableMap.get(entity));
            if (!append) {
                Table sourceTable = entry.getValue();
                log.info("Upsert " + entity.getBatchStore() + " table " + sourceTable.getName());
                Table oldTable = dataCollectionProxy.getTable(configuration.getCustomerSpace().toString(),
                        entity.getServingStore());
                dataCollectionProxy.upsertTable(configuration.getCustomerSpace().toString(), sourceTable.getName(),
                        entity.getServingStore());
                if (oldTable != null) {
                    log.info("Removing old batch store from redshift " + oldTable.getName());
                    redshiftService.dropTable(AvroUtils.getAvroFriendlyString(oldTable.getName()));
                }
            }
        }
    }

    private void exportData(Table sourceTable, String targetTableName, TableRoleInCollection tableRole, boolean append) {
        EaiJobConfiguration exportConfig = setupExportConfig(sourceTable, targetTableName, tableRole, append);
        AppSubmission submission = eaiProxy.submitEaiJob(exportConfig);
        putStringValueInContext(EXPORT_DATA_APPLICATION_ID, submission.getApplicationIds().get(0));
        waitForAppId(submission.getApplicationIds().get(0));
    }

    private String renameTable(Map.Entry<BusinessEntity, Table> entry, boolean append) {
        Table sourceTable = entry.getValue();
        String oldName = sourceTable.getName();
        String goodName = oldName;
        if (!append) {
            String prefix = String.join("_", configuration.getCustomerSpace().getTenantId(), entry.getKey().name());
            goodName = NamingUtils.timestamp(prefix);
        }
        if (!goodName.equalsIgnoreCase(oldName)) {
            log.info("Renaming table " + sourceTable.getName() + " to " + goodName);
            metadataProxy.updateTable(configuration.getCustomerSpace().toString(), goodName, sourceTable);
            sourceTable.setName(goodName);
        }
        return goodName;
    }

    private ExportConfiguration setupExportConfig(Table sourceTable, String targetTableName,
            TableRoleInCollection tableRole, boolean append) {
        HdfsToRedshiftConfiguration exportConfig = configuration.getHdfsToRedshiftConfiguration();
        exportConfig.setExportInputPath(sourceTable.getExtractsDirectory() + "/*.avro");
        exportConfig.setExportTargetPath(sourceTable.getName());
        exportConfig.setNoSplit(true);
        exportConfig.setAppend(append);
        exportConfig.setExportDestination(ExportDestination.REDSHIFT);
        RedshiftTableConfiguration redshiftTableConfig = exportConfig.getRedshiftTableConfiguration();
        redshiftTableConfig.setDistStyle(RedshiftTableConfiguration.DistStyle.Key);
        redshiftTableConfig.setDistKey(tableRole.getPrimaryKey().name());
        redshiftTableConfig.setSortKeyType(RedshiftTableConfiguration.SortKeyType.Compound);
        redshiftTableConfig.setSortKeys(tableRole.getForeignKeysAsStringList());
        redshiftTableConfig.setTableName(targetTableName);
        redshiftTableConfig.setJsonPathPrefix(
                String.format("%s/jsonpath/%s.jsonpath", RedshiftUtils.AVRO_STAGE, sourceTable.getName()));
        return exportConfig;
    }

}
