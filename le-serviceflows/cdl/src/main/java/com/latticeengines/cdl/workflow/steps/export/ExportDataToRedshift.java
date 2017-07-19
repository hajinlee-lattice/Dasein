package com.latticeengines.cdl.workflow.steps.export;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
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

    private String customerSpace;
    
    private boolean createNew;

    @Override
    public void execute() {
        log.info("Inside ExportData execute()");
        customerSpace = configuration.getCustomerSpace().toString();
        entityTableMap = getMapObjectFromContext(TABLE_GOING_TO_REDSHIFT, BusinessEntity.class, Table.class);
        createNew = configuration.getHdfsToRedshiftConfiguration().isCreateNew();
        if (entityTableMap == null) {
            entityTableMap = configuration.getSourceTables();
        }
        for (Map.Entry<BusinessEntity, Table> entry : entityTableMap.entrySet()) {
            String targetName = renameTable(entry);
            log.info("Uploading to redshift table " + targetName + ", createNew=" + createNew);
            exportData(entry.getValue(), targetName, entry.getKey().getServingStore());
        }
    }

    @Override
    public void onExecutionCompleted() {
        if (createNew) {
            for (Map.Entry<BusinessEntity, Table> entry : entityTableMap.entrySet()) {
                BusinessEntity entity = entry.getKey();
                Table sourceTable = entry.getValue();
                log.info("Upsert " + entity.getServingStore() + " table " + sourceTable.getName());
                Table oldTable = dataCollectionProxy.getTable(customerSpace, entity.getServingStore());
                dataCollectionProxy.upsertTable(customerSpace, sourceTable.getName(), entity.getServingStore());
                if (oldTable != null) {
                    //TODO: need to change after we have version
                    log.info("Removing old batch store from redshift " + oldTable.getName());
                    redshiftService.dropTable(AvroUtils.getAvroFriendlyString(oldTable.getName()));
                }
            }
        }
    }

    private void exportData(Table sourceTable, String targetTableName, TableRoleInCollection tableRole) {
        EaiJobConfiguration exportConfig = setupExportConfig(sourceTable, targetTableName, tableRole);
        AppSubmission submission = eaiProxy.submitEaiJob(exportConfig);
        putStringValueInContext(EXPORT_DATA_APPLICATION_ID, submission.getApplicationIds().get(0));
        waitForAppId(submission.getApplicationIds().get(0));
    }

    private String renameTable(Map.Entry<BusinessEntity, Table> entry) {
        BusinessEntity entity = entry.getKey();
        Table sourceTable = entry.getValue();
        String goodName;
        if (StringUtils.isNotBlank(configuration.getTargetTableName())) {
            log.info("Enforce target table name to be " + configuration.getTargetTableName());
            goodName = configuration.getTargetTableName();
        } else {
            String prefix = String.join("_", CustomerSpace.parse(customerSpace).getTenantId(), entity.name());
            goodName = NamingUtils.timestamp(prefix);
            log.info("Generated a new target table name: " + goodName);
        }
        if (!createNew) { // upserting
            Table oldTable = dataCollectionProxy.getTable(customerSpace, entity.getServingStore());
            if (oldTable == null) {
                log.warn("Table for " + entity.getServingStore() + " does not exists. Switch to not append.");
                createNew = true; // it is essentially creating new
            } else {
                goodName = oldTable.getName();
            }
        }
        if (createNew) {
            log.info("Renaming table " + sourceTable.getName() + " to " + goodName);
            metadataProxy.updateTable(customerSpace, goodName, sourceTable);
            sourceTable.setName(goodName);
        }
        return goodName;
    }

    private ExportConfiguration setupExportConfig(Table sourceTable, String targetTableName,
            TableRoleInCollection tableRole) {
        HdfsToRedshiftConfiguration exportConfig = configuration.getHdfsToRedshiftConfiguration();
        exportConfig.setExportInputPath(sourceTable.getExtractsDirectory() + "/*.avro");
        exportConfig.setExportTargetPath(sourceTable.getName());
        exportConfig.setNoSplit(true);
        exportConfig.setExportDestination(ExportDestination.REDSHIFT);
        RedshiftTableConfiguration redshiftTableConfig = exportConfig.getRedshiftTableConfiguration();
        redshiftTableConfig.setDistStyle(RedshiftTableConfiguration.DistStyle.Key);
        redshiftTableConfig.setDistKey(tableRole.getPrimaryKey().name());
        redshiftTableConfig.setSortKeyType(RedshiftTableConfiguration.SortKeyType.Compound);
        redshiftTableConfig.setSortKeys(tableRole.getForeignKeysAsStringList());
        redshiftTableConfig.setTableName(targetTableName);
        redshiftTableConfig.setJsonPathPrefix(
                String.format("%s/jsonpath/%s.jsonpath", RedshiftUtils.AVRO_STAGE, targetTableName));
        return exportConfig;
    }

}
