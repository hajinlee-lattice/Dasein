package com.latticeengines.propdata.service.db;

import org.apache.hadoop.yarn.api.records.ApplicationId;

public interface PropDataJobService {

    ApplicationId importData(String table, String targetDir, String queue, String customer, String splitCols,
            int numMappers, String jdbcUrl);

    ApplicationId importData(String table, String targetDir, String queue, String customer, String splitCols,
            String jdbcUrl);

    ApplicationId exportData(String table, String targetDir, String queue, String customer, String splitCols,
            String jdbcUrl);

    ApplicationId exportData(String table, String targetDir, String queue, String customer, String splitCols,
            int numMappers, String jdbcUrl);

}
