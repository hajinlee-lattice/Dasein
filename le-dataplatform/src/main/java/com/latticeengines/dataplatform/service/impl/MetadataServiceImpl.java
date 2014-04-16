package com.latticeengines.dataplatform.service.impl;

import java.util.Map;

import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.exposed.domain.DataSchema;
import com.latticeengines.dataplatform.exposed.domain.DbCreds;
import com.latticeengines.dataplatform.service.MetadataService;
import com.latticeengines.dataplatform.service.impl.metadata.MetadataProvider;

@Component("metadataService")
public class MetadataServiceImpl implements MetadataService {
    
    @Value("${dataplatform.dbtype}")
    private String activeDbType;

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
        MetadataProvider provider = metadataProviders.get(activeDbType);
        return provider.getSchema(creds, tableName);
    }

    @Override
    public String getJdbcConnectionUrl(DbCreds creds) {
        MetadataProvider provider = metadataProviders.get(activeDbType);
        return provider.getConnectionString(creds);
    }
}
