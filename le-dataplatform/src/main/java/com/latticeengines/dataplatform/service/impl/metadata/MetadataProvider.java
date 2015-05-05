package com.latticeengines.dataplatform.service.impl.metadata;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.sqoop.orm.AvroSchemaGenerator;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcUtils;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ConnManager;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modeling.DbCreds;

@SuppressWarnings("deprecation")
public abstract class MetadataProvider {

    private AvroSchemaGenerator avroSchemaGenerator;

    public abstract String getName();
    
    public abstract String getDriverName();

    public abstract ConnManager getConnectionManager(SqoopOptions options);
    
    public abstract Long getRowCount(JdbcTemplate jdbcTemplate, String tableName);
    
    public abstract Long getDataSize(JdbcTemplate jdbcTemplate, String tableName);

    public abstract void createNewEmptyTableFromExistingOne(JdbcTemplate jdbcTemplate, String newTable, String oldTable);

    public abstract void dropTable(JdbcTemplate jdbcTemplate, String table);

    public abstract List<String> showTable(JdbcTemplate jdbcTemplate, String table);
    
    public abstract String getDriverClass();
    
    public abstract String getJdbcUrlTemplate();

    public Schema getSchema(DbCreds dbCreds, String tableName) {
        SqoopOptions options = new SqoopOptions();
        options.setConnectString(getConnectionString(dbCreds));
        ConnManager connManager = getConnectionManager(options);
        avroSchemaGenerator = new AvroSchemaGenerator(options, connManager, tableName);
        try {
            return avroSchemaGenerator.generate();
        } catch (IOException e) {
            return null;
        }
    }

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
            url = creds.getPassword() != null ? url.replaceFirst("\\$\\$PASSWD\\$\\$", creds.getPassword()) : url;
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
        return replaceUrlWithParamsAndTestConnection(getJdbcUrlTemplate(), getDriverClass(), creds);
    }

}
