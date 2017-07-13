package com.latticeengines.cdl.workflow.steps.export;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.eai.EaiJobConfiguration;
import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.export.ExportDataToRedshiftConfiguration;
import com.latticeengines.proxy.exposed.eai.EaiProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.redshiftdb.exposed.utils.RedshiftUtils;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("exportDataToRedshift")
public class ExportDataToRedshift extends BaseWorkflowStep<ExportDataToRedshiftConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ExportDataToRedshift.class);

    @Autowired
    private EaiProxy eaiProxy;

    @Autowired
    private MetadataProxy metadataProxy;

    private Map<BusinessEntity, Table> entityTableMap;

    @Override
    public void execute() {
        log.info("Inside ExportData execute()");
        entityTableMap = getMapObjectFromContext(TABLE_GOING_TO_REDSHIFT, BusinessEntity.class, Table.class);
        if (entityTableMap == null) {
            entityTableMap = configuration.getSourceTables();
        }
        for (Map.Entry<BusinessEntity, Table> entry : entityTableMap.entrySet()) {
            renameTable(entry);
            exportData(entry.getValue());
        }

    }

    @Override
    public void onExecutionCompleted() {
        boolean dropSourceTable = Boolean.TRUE.equals(configuration.getDropSourceTable());
        if (dropSourceTable) {
            for (Map.Entry<BusinessEntity, Table> entry : entityTableMap.entrySet()) {
                Table sourceTable = entry.getValue();
                log.info("Drop source table " + sourceTable.getName());
                metadataProxy.deleteTable(configuration.getCustomerSpace().toString(), sourceTable.getName());
            }
        }
    }

    private void exportData(Table sourceTable) {
        EaiJobConfiguration exportConfig = setupExportConfig(sourceTable);
        AppSubmission submission = eaiProxy.submitEaiJob(exportConfig);
        putStringValueInContext(EXPORT_DATA_APPLICATION_ID, submission.getApplicationIds().get(0));
        waitForAppId(submission.getApplicationIds().get(0));
    }

    private void renameTable(Map.Entry<BusinessEntity, Table> entry) {
        Table sourceTable = entry.getValue();
        String oldName = sourceTable.getName();
        String goodName = String.join("_", configuration.getCustomerSpace().getTenantId(), entry.getKey().name());
        if (!goodName.equalsIgnoreCase(oldName)) {
            log.info("Renaming table " + sourceTable.getName() + " to " + goodName);
            metadataProxy.updateTable(configuration.getCustomerSpace().toString(), goodName, sourceTable);
            sourceTable.setName(goodName);
        }
    }

    private ExportConfiguration setupExportConfig(Table sourceTable) {
        HdfsToRedshiftConfiguration exportConfig = configuration.getHdfsToRedshiftConfiguration();
        exportConfig.setExportInputPath(sourceTable.getExtractsDirectory() + "/*.avro");
        exportConfig.setExportTargetPath(sourceTable.getName());
        exportConfig.setNoSplit(true);
        exportConfig.setExportDestination(ExportDestination.REDSHIFT);
        RedshiftTableConfiguration redshiftTableConfig = exportConfig.getRedshiftTableConfiguration();
        redshiftTableConfig.setTableName(sourceTable.getName());
        redshiftTableConfig.setJsonPathPrefix(
                String.format("%s/jsonpath/%s.jsonpath", RedshiftUtils.AVRO_STAGE, sourceTable.getName()));
        return exportConfig;
    }

}
