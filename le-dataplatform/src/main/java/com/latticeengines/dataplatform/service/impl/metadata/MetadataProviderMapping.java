package com.latticeengines.dataplatform.service.impl.metadata;

import java.util.HashMap;
import java.util.Map;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MetadataProviderMapping {

    @Bean
    public Map<String, MetadataProvider> metadataProviders() {
        Map<String, MetadataProvider> mappings = new HashMap<String, MetadataProvider>();
        SQLServerMetadataProvider ssMdProvider = new SQLServerMetadataProvider();
        MySQLServerMetadataProvider mysqlMdProvider = new MySQLServerMetadataProvider();
        mappings.put(ssMdProvider.getName(), ssMdProvider);
        mappings.put(ssMdProvider.getDriverName(), ssMdProvider);
        mappings.put(mysqlMdProvider.getName(), mysqlMdProvider);
        mappings.put(mysqlMdProvider.getDriverName(), mysqlMdProvider);
        return mappings;
    }
}
