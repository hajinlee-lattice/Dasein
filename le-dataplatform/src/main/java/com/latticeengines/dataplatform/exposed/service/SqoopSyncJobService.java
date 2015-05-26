package com.latticeengines.dataplatform.exposed.service;

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.modeling.DbCreds;

public interface SqoopSyncJobService {

    ApplicationId importData(String table, String targetDir, DbCreds creds, String queue, String customer,
            List<String> splitCols, String columnsToInclude);

    ApplicationId importData(String table, String targetDir, DbCreds creds, String queue, String customer,
            List<String> splitCols, String columnsToInclude, int numMappers, Properties props);

    ApplicationId exportData(String table, String sourceDir, DbCreds creds, String queue, String customer);

    ApplicationId exportData(String table, String sourceDir, DbCreds creds, String queue, String customer, int numMappers);

    ApplicationId exportData(String table, String sourceDir, DbCreds creds, String queue, String customer, int numMappers, String javaColumnTypeMappings);

    void eval(String sql, String assignedQueue, String jobName, String connectionString);

    ApplicationId exportData(String table, String sourceDir, DbCreds creds, String queue, String customer,
            int numMappers, String javaColumnTypeMappings, String exportColumns);

}
