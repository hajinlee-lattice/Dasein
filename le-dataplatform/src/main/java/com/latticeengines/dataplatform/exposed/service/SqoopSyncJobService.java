package com.latticeengines.dataplatform.exposed.service;

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;
import com.latticeengines.domain.exposed.dataplatform.SqoopImporter;
import com.latticeengines.domain.exposed.modeling.DbCreds;

public interface SqoopSyncJobService {

    ApplicationId importData(String table, String targetDir, DbCreds creds, String queue, String customer,
            List<String> splitCols, String columnsToInclude, int numMappers, Properties props);

    ApplicationId importData(String table, String targetDir, DbCreds creds, String queue, String customer,
            List<String> splitCols, String columnsToInclude);

    ApplicationId importData(String table, String targetDir, DbCreds creds, String queue, String customer,
            List<String> splitCols, String columnsToInclude, int numMappers);

    ApplicationId importData(String table, String query, String targetDir, DbCreds creds, String queue, String customer,
            List<String> splitCols, String columnsToInclude, int numMappers, Properties props);

    ApplicationId importData(SqoopImporter importer);

    ApplicationId importDataSync(String table, String targetDir, DbCreds creds, String queue, String customer,
            List<String> splitCols, String columnsToInclude, int numMappers);

    ApplicationId importDataSyncWithWhereCondition(String table, String targetDir, DbCreds creds, String queue,
            String customer, List<String> splitCols, String columnsToInclude, String whereCondition, int numMappers);

    ApplicationId exportData(String table, String sourceDir, DbCreds creds, String queue, String customer);

    ApplicationId exportData(String table, String sourceDir, DbCreds creds, String queue, String customer,
            int numMappers);

    ApplicationId exportData(String table, String sourceDir, DbCreds creds, String queue, String customer,
            int numMappers, String javaColumnTypeMappings);

    ApplicationId exportData(SqoopExporter exporter);

    ApplicationId exportDataSync(String table, String sourceDir, DbCreds creds, String queue, String customer,
            int numMappers, String javaColumnTypeMappings);

    ApplicationId exportDataSync(String table, String sourceDir, DbCreds creds, String queue, String customer,
            int numMappers, String javaColumnTypeMappings, String exportColumns);

    ApplicationId exportDataSync(String table, String sourceDir, DbCreds creds, String queue, String customer,
            int numMappers, String javaColumnTypeMappings, String exportColumns, List<String> otherOptions);

    void eval(String sql, String assignedQueue, String jobName, DbCreds creds);

    void eval(String sql, String assignedQueue, String jobName, String connectionString);

    ApplicationId importDataForQuery(String query, String dataHdfsPath, DbCreds creds, String assignedQueue,
            String customer, List<String> keyCols, String columnsToInclude);

    ApplicationId importDataForQuery(String query, String targetDir, DbCreds creds, String queue, String customer,
            List<String> splitCols, String columnsToInclude, int numMappers, Properties props);
}
