package com.latticeengines.db.service.impl.metadata;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcUtils;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modeling.DbCreds;

public abstract class MetadataProvider {

    public abstract String getName();

    public abstract String getDriverName();

    public abstract Long getRowCount(JdbcTemplate jdbcTemplate, String tableName);

    public abstract Long getDataSize(JdbcTemplate jdbcTemplate, String tableName);

    public abstract void createNewEmptyTableFromExistingOne(JdbcTemplate jdbcTemplate, String newTableName,
            String oldTableName);

    public abstract void dropTable(JdbcTemplate jdbcTemplate, String tableName);

    public abstract List<String> showTable(JdbcTemplate jdbcTemplate, String tableName);

    public abstract void addPrimaryKeyColumn(JdbcTemplate jdbcTemplate, String tableName, String pid);

    public abstract String getDriverClass();

    public abstract String getJdbcUrlTemplate();

    public abstract String getConnectionUrl(DbCreds creds);

    public abstract String getConnectionUserName(DbCreds creds);

    public abstract String getConnectionPassword(DbCreds creds);

    public String replaceUrlWithParamsAndTestConnection(String url, String driverClass, DbCreds creds) {
        Connection conn = null;
        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException e) {
            throw new LedpException(LedpCode.LEDP_11000, e, new String[] { driverClass });
        }

        try {
            url = creds.getHost() != null ? url.replaceFirst("\\$\\$HOST\\$\\$", creds.getHost()) : url;
            url = url.replaceFirst("\\$\\$PORT\\$\\$", Integer.toString(creds.getPort()));
            url = creds.getDb() != null ? url.replaceFirst("\\$\\$DB\\$\\$", creds.getDb()) : url;
            url = creds.getUser() != null ? url.replaceFirst("\\$\\$USER\\$\\$", creds.getUser()) : url;
            url = creds.getDecryptedPassword() != null ? url.replaceFirst("\\$\\$PASSWD\\$\\$",
                    creds.getDecryptedPassword()) : url;
            url = creds.getInstance() != null ? url.replaceFirst("\\$\\$INSTANCE\\$\\$", creds.getInstance()) : url;

            conn = DriverManager.getConnection(url);
        } catch (SQLException e) {
            throw new LedpException(LedpCode.LEDP_11001, e);
        } finally {
            if (conn != null) {
                JdbcUtils.closeConnection(conn);
            }
        }
        return url;
    }

    public String getConnectionString(DbCreds creds) {
        String url = creds.getJdbcUrl();
        String driverClass = creds.getDriverClass();

        if (StringUtils.isEmpty(url)) {
            url = getJdbcUrlTemplate();
        }
        if (StringUtils.isEmpty(driverClass)) {
            driverClass = getDriverClass();
        }
        return replaceUrlWithParamsAndTestConnection(url, driverClass, creds);
    }

    public abstract void createNewTable(JdbcTemplate jdbcTemplate, String tableName, String columnInfo);

    public abstract int insertRow(JdbcTemplate jdbcTemplate, String tableName, String columnStatement, Object... args);

    public abstract void createNewTableFromExistingOne(JdbcTemplate jdbcTemplate, String newTableName,
            String oldTableName);

    public abstract Long getPositiveEventCount(JdbcTemplate jdbcTemplate, String tableName, String eventColName);

    public abstract boolean checkIfColumnExists(JdbcTemplate jdbcTemplate, String tableName, String column);

    public abstract List<String> getDistinctColumnValues(JdbcTemplate jdbcTemplate, String tableName, String column);

    public abstract String getConnectionUrl(String completeUrl);

    public abstract String getConnectionUserName(String completeUrl);

    public abstract String getConnectionPassword(String completeUrl);

}
