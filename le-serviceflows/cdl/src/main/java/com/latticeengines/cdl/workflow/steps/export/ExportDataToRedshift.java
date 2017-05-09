package com.latticeengines.cdl.workflow.steps.export;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.eai.EaiJobConfiguration;
import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.metadata.JdbcStorage;
import com.latticeengines.domain.exposed.metadata.JdbcStorage.DatabaseName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.proxy.exposed.eai.EaiProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
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
        // saveToContext();
    }

    private ExportConfiguration setupExportConfig() {
        Table sourceTable = getObjectFromContext(EVENT_TABLE, Table.class);
        if (sourceTable == null) {
            sourceTable = configuration.getSourceTable();
        }
        HdfsToRedshiftConfiguration exportConfig = new HdfsToRedshiftConfiguration();
        exportConfig.setExportFormat(ExportFormat.AVRO);
        exportConfig.setExportInputPath(sourceTable.getExtractsDirectory() + "/*.avro");
        exportConfig.setRedshiftTableConfiguration(configuration.getRedshiftTableConfiguration());
        return exportConfig;
    }

    @Override
    public void onExecutionCompleted() {
        String bucketedTableName = configuration.getRedshiftTableConfiguration().getTableName();
        Table bucketedTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(), bucketedTableName);
        JdbcStorage storage = new JdbcStorage();
        storage.setDatabaseName(DatabaseName.REDSHIFT);
        storage.setTableNameInStorage(bucketedTable.getStorageMechanism().getTableNameInStorage());
        bucketedTable.setStorageMechanism(storage);
        metadataProxy.updateTable(configuration.getCustomerSpace().toString(), bucketedTableName, bucketedTable);
        putObjectInContext(EVENT_TABLE, bucketedTable);
    }
}
