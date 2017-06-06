package com.latticeengines.cdl.workflow.steps.export;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.eai.EaiJobConfiguration;
import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.metadata.JdbcStorage;
import com.latticeengines.domain.exposed.metadata.JdbcStorage.DatabaseName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.proxy.exposed.eai.EaiProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.redshiftdb.exposed.utils.RedshiftUtils;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("exportDataToRedshift")
public class ExportDataToRedshift extends BaseWorkflowStep<ExportDataToRedshiftConfiguration> {

    private static final Log log = LogFactory.getLog(ExportDataToRedshift.class);

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private EaiProxy eaiProxy;

    @Override
    public void execute() {
        log.info("Inside ExportData execute()");
        exportData();
    }

    private void exportData() {
        EaiJobConfiguration exportConfig = setupExportConfig();
        AppSubmission submission = eaiProxy.submitEaiJob(exportConfig);
        putStringValueInContext(EXPORT_DATA_APPLICATION_ID, submission.getApplicationIds().get(0).toString());
        waitForAppId(submission.getApplicationIds().get(0).toString());
    }

    private ExportConfiguration setupExportConfig() {
        Table sourceTable = getObjectFromContext(CONSOLIDATE_CONSOLIDATED_TABLE, Table.class);
        if (sourceTable == null) {
            sourceTable = configuration.getSourceTable();
        }
        HdfsToRedshiftConfiguration exportConfig = configuration.getHdfsToRedshiftConfiguration();
        exportConfig.setExportInputPath(sourceTable.getExtractsDirectory() + "/*.avro");
        exportConfig.setExportTargetPath(sourceTable.getName());
        exportConfig.setExportDestination(ExportDestination.RedShift);
        RedshiftTableConfiguration redshiftTableConfig = exportConfig.getRedshiftTableConfiguration();
        redshiftTableConfig.setTableName(sourceTable.getName());
        redshiftTableConfig.setJsonPathPrefix(
                String.format("%s/jsonpath/%s.jsonpath", RedshiftUtils.AVRO_STAGE, sourceTable.getName()));
        return exportConfig;
    }

    @Override
    public void onExecutionCompleted() {
        Table table = getObjectFromContext(CONSOLIDATE_CONSOLIDATED_TABLE, Table.class);
        if (table == null) {
            table = configuration.getSourceTable();
        }
        JdbcStorage storage = new JdbcStorage();
        storage.setDatabaseName(DatabaseName.REDSHIFT);
        storage.setTableNameInStorage(table.getName());
        table.setStorageMechanism(storage);
        metadataProxy.updateTable(configuration.getCustomerSpace().toString(), table.getName(), table);
        putObjectInContext(EVENT_TABLE, table);
    }
}
