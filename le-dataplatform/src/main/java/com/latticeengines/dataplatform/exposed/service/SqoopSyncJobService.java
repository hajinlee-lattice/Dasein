package com.latticeengines.dataplatform.exposed.service;

import org.apache.hadoop.yarn.api.records.ApplicationId;

public interface SqoopSyncJobService {

    ApplicationId importData(String table, String targetDir, String queue, String customer, String splitCols,
            int numMappers, String jdbcUrl);

    ApplicationId importData(String table, String targetDir, String queue, String customer, String splitCols,
            String jdbcUrl);

    ApplicationId exportData(String table, String sourceDir, String queue, String customer, String jdbcUrl);

    ApplicationId exportData(String table, String sourceDir, String queue, String customer, int numMappers,
            String jdbcUrl);

    ApplicationId exportData(String table, String sourceDir, String queue, String customer, int numMappers,
            String jdbcUrl, String javaTypeMappings);

    void eval(String sql, String assignedQueue, String jobName, int i, String connectionString);

}
