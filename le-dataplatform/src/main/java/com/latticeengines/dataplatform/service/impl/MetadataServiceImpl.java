package com.latticeengines.dataplatform.service.impl;

import java.util.Map;

import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.service.MetadataService;
import com.latticeengines.dataplatform.service.impl.metadata.MetadataProvider;
import com.latticeengines.domain.exposed.modeling.DataSchema;
import com.latticeengines.domain.exposed.modeling.DbCreds;

@Component("metadataService")
public class MetadataServiceImpl implements MetadataService {
    
    private Map<String, MetadataProvider> metadataProviders;

    @Autowired
    public void setMetadataProviders(
            @Value("#{metadataProviders}") Map<String, MetadataProvider> metadataProviders) {
        this.metadataProviders = metadataProviders;
    }

    @Override
    public DataSchema createDataSchema(DbCreds creds, String tableName) {
        return new DataSchema(getAvroSchema(creds, tableName));
    }

    @Override
    public Schema getAvroSchema(DbCreds creds, String tableName) {
        String dbType = creds.getType();
        MetadataProvider provider = metadataProviders.get(dbType);
        return provider.getSchema(creds, tableName);
    }

    @Override
    public String getJdbcConnectionUrl(DbCreds creds) {
        String dbType = creds.getType();
        MetadataProvider provider = metadataProviders.get(dbType);
        return provider.getConnectionString(creds);
    }
}
