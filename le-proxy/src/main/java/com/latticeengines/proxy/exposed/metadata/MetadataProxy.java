package com.latticeengines.proxy.exposed.metadata;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.network.exposed.metadata.MetadataInterface;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component("metadataProxy")
public class MetadataProxy extends BaseRestApiProxy implements MetadataInterface {
    
    public MetadataProxy() {
        super("metadata");
    }

    @Override
    public Boolean resetTables(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/tables/reset", customerSpace);
        return post("reset", url, null, Boolean.class);

    }

    @Override
    public Boolean createTable(String customerSpace, String tableName, Table table) {
        String url = constructUrl("/customerspaces/{customerSpace}/importtables/{tableName}", customerSpace, tableName);
        return post("createTable", url, table, Boolean.class);
    }

}
