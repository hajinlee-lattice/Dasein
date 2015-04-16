package com.latticeengines.dataplatform.service.impl.metadata;

import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.SQLServerManager;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modeling.DbCreds;

@SuppressWarnings("deprecation")
public class SQLServerMetadataProvider extends MetadataProvider {

    public SQLServerMetadataProvider() {
    }

    public String getName() {
        return "SQLServer";
    }

    public String getConnectionString(DbCreds creds) {
        String url = "jdbc:sqlserver://$$HOST$$:$$PORT$$;databaseName=$$DB$$;user=$$USER$$;password=$$PASSWD$$";
        String driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException e) {
            throw new LedpException(LedpCode.LEDP_11000, e,
                    new String[] { driverClass });
        }
        return replaceUrlWithParamsAndTestConnection(url, creds);
    }

    public ConnManager getConnectionManager(SqoopOptions options) {
        return new SQLServerManager(options);
    }

    @Override
    public Long getRowCount(JdbcTemplate jdbcTemplate, String tableName) {
        Map<String, Object> resMap = jdbcTemplate.queryForMap("EXEC sp_spaceused N'" + tableName + "'");
        String rowCount = (String) resMap.get("rows");
        return Long.valueOf(rowCount.trim());
    }

    @Override
    public Long getDataSize(JdbcTemplate jdbcTemplate, String tableName) {
        Map<String, Object> resMap = jdbcTemplate.queryForMap("EXEC sp_spaceused N'" + tableName + "'");
        String dataSize = ((String) resMap.get("data")).trim().split(" ")[0];
        return 1024 * Long.valueOf(dataSize);
    }

    @Override
    public String getDriverName() {
        return "Microsoft JDBC Driver 4.0 for SQL Server";
    }

    @Override
    public String createNewEmptyTableFromExistingOne(String newTable, String oldTable){
        return "SELECT * INTO " + newTable + " select * from " + oldTable + " WHERE 1 = 0";
    }

}
