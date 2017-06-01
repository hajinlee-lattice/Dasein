package com.latticeengines.cdl.workflow.steps.export;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.eai.EaiJobConfiguration;
import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.metadata.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.JdbcStorage;
import com.latticeengines.domain.exposed.metadata.JdbcStorage.DatabaseName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration.DistStyle;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration.SortKeyType;
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

    @Value("${aws.s3.bucket}")
    private String s3Bucket;

    @Override
    public void execute() {
        log.info("Inside ExportData execute()");
        exportData();
    }

    private void exportData() {
        List<Table> tables = configuration.getSourceTables();
        DataFeedExecution execution = getObjectFromContext(EXECUTION, DataFeedExecution.class);
        if (execution != null) {
            tables = execution.getRunTables();
        }
        Map<String, String> tableNameToAppId = new HashMap<>();
        for (Table table : tables) {
            EaiJobConfiguration exportConfig = setupExportConfig(table);
            AppSubmission submission = eaiProxy.submitEaiJob(exportConfig);
            tableNameToAppId.put(table.getName(), submission.getApplicationIds().get(0).toString());
        }
        for (String tableName : tableNameToAppId.keySet()) {
            waitForAppId(tableNameToAppId.get(tableName));
            finalize(tableName);
        }
    }

    private ExportConfiguration setupExportConfig(Table sourceTable) {
        HdfsToRedshiftConfiguration exportConfig = new HdfsToRedshiftConfiguration();
        exportConfig.setExportFormat(ExportFormat.AVRO);
        exportConfig.setExportInputPath(sourceTable.getExtractsDirectory() + "/*.avro");
        exportConfig.setCleanupS3(configuration.isCleanupS3());
        if (configuration.isInitialLoad()) {
            exportConfig.setCreateNew(true);
            exportConfig.setAppend(true);
        }
        RedshiftTableConfiguration redshiftTableConfig = new RedshiftTableConfiguration();
        redshiftTableConfig.setTableName(sourceTable.getName());
        redshiftTableConfig.setS3Bucket(s3Bucket);
        redshiftTableConfig.setJsonPathPrefix(
                String.format("%s/jsonpath/%s.jsonpath", RedshiftUtils.AVRO_STAGE, sourceTable.getName()));
        redshiftTableConfig.setDistStyle(DistStyle.Key);
        redshiftTableConfig.setDistKey("LatticeAccountId");
        redshiftTableConfig.setSortKeyType(SortKeyType.Compound);
        redshiftTableConfig.setSortKeys(Collections.<String> singletonList("LatticeAccountId"));
        exportConfig.setRedshiftTableConfiguration(redshiftTableConfig);
        return exportConfig;
    }

    public void finalize(String tableName) {
        Table table = metadataProxy.getTable(configuration.getCustomerSpace().toString(), tableName);
        JdbcStorage storage = new JdbcStorage();
        storage.setDatabaseName(DatabaseName.REDSHIFT);
        storage.setTableNameInStorage(tableName);
        table.setStorageMechanism(storage);
        metadataProxy.updateTable(configuration.getCustomerSpace().toString(), tableName, table);
    }
}
