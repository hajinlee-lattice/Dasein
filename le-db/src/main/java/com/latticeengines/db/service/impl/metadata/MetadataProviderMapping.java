package com.latticeengines.db.service.impl.metadata;

import java.util.HashMap;
import java.util.Map;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MetadataProviderMapping {

    @Bean
    public Map<String, MetadataProvider> metadataProviders() {
        Map<String, MetadataProvider> mappings = new HashMap<>();
        SQLServerMetadataProvider ssMdProvider = new SQLServerMetadataProvider();
        MySQLServerMetadataProvider mysqlMdProvider = new MySQLServerMetadataProvider();
        GenericJdbcMetadataProvider jdbcProvider = new GenericJdbcMetadataProvider();
        mappings.put(ssMdProvider.getName(), ssMdProvider);
        mappings.put(ssMdProvider.getDriverName(), ssMdProvider);
        mappings.put(mysqlMdProvider.getName(), mysqlMdProvider);
        mappings.put(mysqlMdProvider.getDriverName(), mysqlMdProvider);
        mappings.put(jdbcProvider.getName(), jdbcProvider);
        mappings.put(jdbcProvider.getDriverName(), jdbcProvider);
        return mappings;
    }
}
