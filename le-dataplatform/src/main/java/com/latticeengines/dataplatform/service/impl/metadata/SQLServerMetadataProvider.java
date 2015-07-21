package com.latticeengines.dataplatform.service.impl.metadata;

import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.SQLServerManager;

@SuppressWarnings("deprecation")
public class SQLServerMetadataProvider extends MetadataProvider {

    public SQLServerMetadataProvider() {
    }

    public String getName() {
        return "SQLServer";
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
    public void createNewTableFromExistingOne(JdbcTemplate jdbcTemplate, String newTable, String oldTable) {
        jdbcTemplate.execute("SELECT * INTO [" + newTable + "] FROM [" + oldTable + "]");
    }

    @Override
    public void createNewEmptyTableFromExistingOne(JdbcTemplate jdbcTemplate, String newTable, String oldTable) {
        jdbcTemplate.execute("SELECT * INTO [" + newTable + "] FROM [" + oldTable + "] WHERE 1 = 0");
    }

    @Override
    public void dropTable(JdbcTemplate jdbcTemplate, String table) {
        jdbcTemplate.execute("IF OBJECT_ID('" + table + "', 'U') IS NOT NULL DROP TABLE [" + table + "]");
    }

    @Override
    public List<String> showTable(JdbcTemplate jdbcTemplate, String table) {
        return jdbcTemplate.queryForList("SELECT [name] FROM SYS.TABLES WHERE [name] = '" + table + "'", String.class);
    }

    @Override
    public String getDriverClass() {
        return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    }

    @Override
    public String getJdbcUrlTemplate() {
        return "jdbc:sqlserver://$$HOST$$:$$PORT$$;databaseName=$$DB$$;user=$$USER$$;password=$$PASSWD$$";
    }

    @Override
    public void addPrimaryKeyColumn(JdbcTemplate jdbcTemplate, String table, String pid) {
        jdbcTemplate.execute("ALTER TABLE [" + table + "] ADD " + pid + " INT IDENTITY");
    }

}
