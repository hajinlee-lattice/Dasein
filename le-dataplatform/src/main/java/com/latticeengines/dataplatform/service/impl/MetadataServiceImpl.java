package com.latticeengines.dataplatform.service.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.dataplatform.service.impl.metadata.MetadataProvider;
import com.latticeengines.domain.exposed.modeling.DataSchema;
import com.latticeengines.domain.exposed.modeling.DbCreds;

@Component("metadataService")
public class MetadataServiceImpl implements MetadataService {

    private Map<String, MetadataProvider> metadataProviders;

    @Autowired
    public void setMetadataProviders(@Value("#{metadataProviders}") Map<String, MetadataProvider> metadataProviders) {
        this.metadataProviders = metadataProviders;
    }

    @Override
    public DataSchema createDataSchema(DbCreds creds, String tableName) {
        return new DataSchema(getAvroSchema(creds, tableName));
    }

    @Override
    public Schema getAvroSchema(DbCreds creds, String tableName) {
        String dbType = creds.getDbType();
        MetadataProvider provider = metadataProviders.get(dbType);
        return provider.getSchema(creds, tableName);
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
        if (!StringUtils.isEmpty(url)) {
            return provider.replaceUrlWithParamsAndTestConnection(url, driverClass, creds);
        }
        return provider.getConnectionString(creds);
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
    public void createNewTableFromExistingOne(JdbcTemplate jdbcTemplate, String newTable, String oldTable) {
        MetadataProvider provider = getProvider(jdbcTemplate);
        provider.createNewTableFromExistingOne(jdbcTemplate, newTable, oldTable);
    }

    @Override
    public void createNewEmptyTableFromExistingOne(JdbcTemplate jdbcTemplate, String newTable, String oldTable) {
        MetadataProvider provider = getProvider(jdbcTemplate);
        provider.createNewEmptyTableFromExistingOne(jdbcTemplate, newTable, oldTable);
    }

    @Override
    public void dropTable(JdbcTemplate jdbcTemplate, String table) {
        MetadataProvider provider = getProvider(jdbcTemplate);
        provider.dropTable(jdbcTemplate, table);
    }

    @Override
    public List<String> showTable(JdbcTemplate jdbcTemplate, String table) {
        MetadataProvider provider = getProvider(jdbcTemplate);
        return provider.showTable(jdbcTemplate, table);
    }

    @Override
    public void addPrimaryKeyColumn(JdbcTemplate jdbcTemplate, String table, String pid) {
        MetadataProvider provider = getProvider(jdbcTemplate);
        provider.addPrimaryKeyColumn(jdbcTemplate, table, pid);
    }

    @Override
    public List<String> getColumnNames(JdbcTemplate jdbcTemplate, String table) {
        return jdbcTemplate.queryForList("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + table + "'", String.class);
    }

    @Override
    public JdbcTemplate constructJdbcTemplate(DbCreds creds){
        DataSource dataSource = new DriverManagerDataSource(this.getJdbcConnectionUrl(creds), creds.getUser(), creds.getPassword());
        return new JdbcTemplate(dataSource);
    }
}
