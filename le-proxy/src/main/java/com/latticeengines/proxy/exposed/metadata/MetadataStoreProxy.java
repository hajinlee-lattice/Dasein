package com.latticeengines.proxy.exposed.metadata;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

import reactor.core.publisher.Flux;

@Component("metadataStoreProxy")
public class MetadataStoreProxy extends MicroserviceRestApiProxy {

    private static final Logger log = LoggerFactory.getLogger(MetadataStoreProxy.class);

    protected MetadataStoreProxy() {
        super("metadata");
    }

    public Flux<ColumnMetadata> getMetadata(String mdsName, String... namespace) {
        String url = constructUrl("/metadatastore/{mdsName}/namespace/{namespace}", mdsName,
                StringUtils.join(namespace, ","));
        return getFlux("get metadata", url, ColumnMetadata.class);
    }

}
