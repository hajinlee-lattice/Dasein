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
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.network.exposed.propdata.ColumnMetadataInterface;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component("columnMetadataProxyMatchapi")
public class ColumnMetadataProxy extends BaseRestApiProxy implements ColumnMetadataInterface {

    private static Log log = LogFactory.getLog(ColumnMetadataProxy.class);
    private static final String DEFAULT = "default";

    private LoadingCache<String, List<ColumnMetadata>> enrichmentColumnsCache;
    private LoadingCache<String, DataCloudVersion> latestDataCloudVersionCache;

    public ColumnMetadataProxy() {
        super(PropertyUtils.getProperty("common.matchapi.url"), "/match/metadata");
        enrichmentColumnsCache = CacheBuilder.newBuilder().maximumSize(20).refreshAfterWrite(10, TimeUnit.MINUTES)
                .build(new CacheLoader<String, List<ColumnMetadata>>() {
                    @Override
                    public List<ColumnMetadata> load(String dataCloudVersion) throws Exception {
                        if (DEFAULT.equals(dataCloudVersion)) {
                            dataCloudVersion = "";
                        }
                        List<ColumnMetadata> columns = requestColumnSelection(Predefined.Enrichment, dataCloudVersion);
                        log.info("Loaded " + columns.size() + " columns into LoadingCache.");
                        return columns;
                    }
                });

        latestDataCloudVersionCache = CacheBuilder.newBuilder().maximumSize(20).refreshAfterWrite(10, TimeUnit.MINUTES)
                .build(new CacheLoader<String, DataCloudVersion>() {
                    @Override
                    public DataCloudVersion load(String compatibleVersion) throws Exception {
                        if (DEFAULT.equals(compatibleVersion)) {
                            compatibleVersion = "";
                        }
                        DataCloudVersion latestVersion = requestLatestVersion(compatibleVersion);
                        log.info("Loaded latest version for compatibleVersion '" + compatibleVersion
                                + "' into LoadingCache: " + (latestVersion == null ? null : latestVersion.getVersion()));
                        return latestVersion;
                    }
                });
    }

    @Override
    public List<ColumnMetadata> columnSelection(Predefined selectName, String dataCloudVersion) {
        if (Predefined.Enrichment.equals(selectName)) {
            try {
                if (StringUtils.isEmpty(dataCloudVersion)) {
                    dataCloudVersion = DEFAULT;
                }
                return enrichmentColumnsCache.get(dataCloudVersion);
            } catch (Exception e) {
                throw new RuntimeException("Failed to get enrichment column metadata from loading cache.", e);
            }
        } else {
            return requestColumnSelection(selectName, dataCloudVersion);
        }
    }

    @Override
    public DataCloudVersion latestVersion(String compatibleVersion) {
        try {
            if (StringUtils.isEmpty(compatibleVersion)) {
                compatibleVersion = DEFAULT;
            }
            return latestDataCloudVersionCache.get(compatibleVersion);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get latest version for dataCloudVersion " //
                    + compatibleVersion + " from loading cache.", e);
        }
    }

    @SuppressWarnings({ "unchecked" })
    private List<ColumnMetadata> requestColumnSelection(Predefined selectName, String dataCloudVersion) {
        String url = constructUrl("/predefined/{selectName}", String.valueOf(selectName.name()));
        if (StringUtils.isNotBlank(dataCloudVersion)) {
            url = constructUrl("/predefined/{selectName}?datacloudversion={dataCloudVersion}",
                    String.valueOf(selectName.name()), dataCloudVersion);
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

    @SuppressWarnings({ "unchecked" })
    public AttributeRepository getAttrRepo() {
        String url = constructUrl("/attrrepo");
        return get("get AM attr repo", url, AttributeRepository.class);
    }

    private DataCloudVersion requestLatestVersion(String compatibleVersion) {
        String url;
        if (StringUtils.isNotBlank(compatibleVersion)) {
            url = constructUrl("/versions/latest?compatibleto={compatibleVersion}", compatibleVersion);
        } else {
            url = constructUrl("/versions/latest");
        }
        DataCloudVersion latestVersion = get("latest version", url, DataCloudVersion.class);
        return latestVersion;
    }
}
