package com.latticeengines.proxy.exposed.matchapi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.network.exposed.propdata.ColumnMetadataInterface;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component("columnMetadataProxyMatchapi")
public class ColumnMetadataProxy extends BaseRestApiProxy implements ColumnMetadataInterface {

    private static Log log = LogFactory.getLog(ColumnMetadataProxy.class);
    private LoadingCache<String, List<ColumnMetadata>> enrichmentColumnsCache;

    public ColumnMetadataProxy() {
        super(PropertyUtils.getProperty("proxy.matchapi.rest.endpoint.hostport"), "/match/metadata");
        enrichmentColumnsCache = CacheBuilder.newBuilder().maximumSize(20).expireAfterWrite(10, TimeUnit.MINUTES)
                .build(new CacheLoader<String, List<ColumnMetadata>>() {
                    public List<ColumnMetadata> load(String dataCloudVersion) throws Exception {
                        List<ColumnMetadata> columns= requestColumnSelection(Predefined.Enrichment, dataCloudVersion);
                        log.info("Loaded " + columns.size() + " columns into LoadingCache.");
                        return columns;
                    }
                });
    }

    @Override
    public List<ColumnMetadata> columnSelection(Predefined selectName, String dataCloudVersion) {
        if (Predefined.Enrichment.equals(selectName)) {
            try {
                return enrichmentColumnsCache.get(dataCloudVersion);
            } catch (Exception e) {
                throw new RuntimeException("Failed to get enrichment column metadata from loading cache.", e);
            }
        } else {
            return requestColumnSelection(selectName, dataCloudVersion);
        }
    }

    @SuppressWarnings({ "unchecked" })
    private List<ColumnMetadata> requestColumnSelection(Predefined selectName, String dataCloudVersion) {
        String url = constructUrl("/predefined/{selectName}", String.valueOf(selectName.name()));
        if (StringUtils.isNotBlank(dataCloudVersion)) {
            url = constructUrl("/predefined/{selectName}?datacloudversion", String.valueOf(selectName.name()),
                    dataCloudVersion);
        }
        List<Map<String, Object>> metadataObjs = get("columnSelection", url, List.class);
        List<ColumnMetadata> metadataList = new ArrayList<>();
        if (metadataObjs == null) {
            return metadataList;
        }

        ObjectMapper mapper = new ObjectMapper();
        try {
            for (Map<String, Object> obj : metadataObjs) {
                ColumnMetadata metadata = mapper.treeToValue(mapper.valueToTree(obj), ColumnMetadata.class);
                metadataList.add(metadata);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return metadataList;
    }

}
