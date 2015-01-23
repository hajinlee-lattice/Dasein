package com.latticeengines.dataplatform.service.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.service.MetadataService;
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
        String dbType = creds.getDBType();
        MetadataProvider provider = metadataProviders.get(dbType);
        return provider.getSchema(creds, tableName);
    }

    @Override
    public String getJdbcConnectionUrl(DbCreds creds) {
        String dbType = creds.getDBType();
        MetadataProvider provider = metadataProviders.get(dbType);
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
        Integer positiveEventCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM " + tableName + " WHERE " + eventColName + " = 1", Integer.class);
        return Long.valueOf(positiveEventCount);
        
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
}
