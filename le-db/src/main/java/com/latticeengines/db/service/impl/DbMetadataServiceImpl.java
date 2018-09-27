package com.latticeengines.db.service.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.service.DbMetadataService;
import com.latticeengines.db.service.impl.metadata.MetadataProvider;
import com.latticeengines.domain.exposed.modeling.DbCreds;

@Component("dbMetadataService")
public class DbMetadataServiceImpl implements DbMetadataService {

    private Map<String, MetadataProvider> metadataProviders;

    @Autowired
    public void setMetadataProviders(@Value("#{metadataProviders}") Map<String, MetadataProvider> metadataProviders) {
        this.metadataProviders = metadataProviders;
    }

    @Override
    public String getJdbcConnectionUrl(DbCreds creds) {
        String dbType = creds.getDbType();
        MetadataProvider provider = metadataProviders.get(dbType);

        String url = creds.getJdbcUrl();
        String driverClass = creds.getDriverClass();

        if (StringUtils.isEmpty(driverClass)) {
            driverClass = provider.getDriverClass();
        }
        if (StringUtils.isNotEmpty(url)) {
            return provider.replaceUrlWithParamsAndTestConnection(url, driverClass, creds);
        }
        return provider.getConnectionString(creds);
    }

    @Override
    public String getConnectionUrl(DbCreds creds) {
        String dbType = creds.getDbType();
        MetadataProvider provider = metadataProviders.get(dbType);
        return provider.getConnectionUrl(this.getJdbcConnectionUrl(creds));
    }

    @Override
    public String getConnectionUserName(DbCreds creds) {
        String dbType = creds.getDbType();
        MetadataProvider provider = metadataProviders.get(dbType);
        return provider.getConnectionUserName(this.getJdbcConnectionUrl(creds));
    }

    @Override
    public String getConnectionPassword(DbCreds creds) {
        String dbType = creds.getDbType();
        MetadataProvider provider = metadataProviders.get(dbType);
        return provider.getConnectionPassword(this.getJdbcConnectionUrl(creds));
    }

    private MetadataProvider getProvider(JdbcTemplate jdbcTemplate) {
        try {
            return metadataProviders.get(jdbcTemplate.getDataSource().getConnection().getMetaData().getDriverName());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Long getRowCount(JdbcTemplate jdbcTemplate, String tableName) {
        MetadataProvider provider = getProvider(jdbcTemplate);
        return provider.getRowCount(jdbcTemplate, tableName);
    }

    @Override
    public Long getPositiveEventCount(JdbcTemplate jdbcTemplate, String tableName, String eventColName) {
        MetadataProvider provider = getProvider(jdbcTemplate);
        return provider.getPositiveEventCount(jdbcTemplate, tableName, eventColName);
    }

    @Override
    public Long getDataSize(JdbcTemplate jdbcTemplate, String tableName) {
        MetadataProvider provider = getProvider(jdbcTemplate);
        return provider.getDataSize(jdbcTemplate, tableName);
    }

    @Override
    public Integer getColumnCount(JdbcTemplate jdbcTemplate, String tableName) {
        int numCols = 0;
        try {
            ResultSet rset = jdbcTemplate.getDataSource().getConnection().getMetaData()
                    .getColumns(null, null, tableName, null);
            while (rset.next()) {
                numCols++;
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return numCols;
    }

    @Override
    public void createNewTableFromExistingOne(JdbcTemplate jdbcTemplate, String newTableName, String oldTableName) {
        MetadataProvider provider = getProvider(jdbcTemplate);
        provider.createNewTableFromExistingOne(jdbcTemplate, newTableName, oldTableName);
    }

    @Override
    public void createNewEmptyTableFromExistingOne(JdbcTemplate jdbcTemplate, String newTableName, String oldTableName) {
        MetadataProvider provider = getProvider(jdbcTemplate);
        provider.createNewEmptyTableFromExistingOne(jdbcTemplate, newTableName, oldTableName);
    }

    @Override
    public void dropTable(JdbcTemplate jdbcTemplate, String tableName) {
        MetadataProvider provider = getProvider(jdbcTemplate);
        provider.dropTable(jdbcTemplate, tableName);
    }

    @Override
    public List<String> showTable(JdbcTemplate jdbcTemplate, String tableName) {
        MetadataProvider provider = getProvider(jdbcTemplate);
        return provider.showTable(jdbcTemplate, tableName);
    }

    @Override
    public void addPrimaryKeyColumn(JdbcTemplate jdbcTemplate, String tableName, String pid) {
        MetadataProvider provider = getProvider(jdbcTemplate);
        provider.addPrimaryKeyColumn(jdbcTemplate, tableName, pid);
    }

    @Override
    public List<String> getColumnNames(JdbcTemplate jdbcTemplate, String tableName) {
        return jdbcTemplate.queryForList("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '"
                + tableName + "'", String.class);
    }

    @Override
    public JdbcTemplate constructJdbcTemplate(DbCreds creds) {
        DataSource dataSource = new DriverManagerDataSource(this.getConnectionUrl(creds), creds.getUser(),
                creds.getDecryptedPassword());
        return new JdbcTemplate(dataSource);
    }

    @Override
    public void createNewTable(JdbcTemplate jdbcTemplate, String tableName, String columnInfo) {
        MetadataProvider provider = getProvider(jdbcTemplate);
        provider.createNewTable(jdbcTemplate, tableName, columnInfo);
    }

    @Override
    public int insertRow(JdbcTemplate jdbcTemplate, String tableName, String columnStatement, Object... args) {
        MetadataProvider provider = getProvider(jdbcTemplate);
        return provider.insertRow(jdbcTemplate, tableName, columnStatement, args);
    }

    @Override
    public boolean checkIfColumnExists(JdbcTemplate jdbcTemplate, String tableName, String column) {
        MetadataProvider provider = getProvider(jdbcTemplate);
        return provider.checkIfColumnExists(jdbcTemplate, tableName, column);
    }

    @Override
    public List<String> getDistinctColumnValues(JdbcTemplate jdbcTemplate, String tableName, String column) {
        MetadataProvider provider = getProvider(jdbcTemplate);
        return provider.getDistinctColumnValues(jdbcTemplate, tableName, column);
    }

    @Override
    public String getConnectionString(DbCreds creds) {
        String dbType = creds.getDbType();
        MetadataProvider provider = metadataProviders.get(dbType);
        return provider.getConnectionString(creds);
    }
}
